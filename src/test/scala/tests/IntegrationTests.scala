package com.sneaksanddata.arcane.stream_pull
package tests

import com.sneaksanddata.arcane.framework.models.schemas.ArcaneType.StringType
import com.sneaksanddata.arcane.framework.models.schemas.{ArcaneSchema, Field, MergeKeyField}
import com.sneaksanddata.arcane.framework.services.pullstream.versioning.PullStreamWatermark
import com.sneaksanddata.arcane.framework.testkit.setups.FrameworkTestSetup.prepareWatermark
import com.sneaksanddata.arcane.framework.testkit.verifications.FrameworkVerificationUtilities.{clearTarget, readTarget}
import com.sneaksanddata.arcane.framework.testkit.zioutils.ZKit.{liveSeed, runOrFail}
import com.sneaksanddata.arcane.pull_stream_plugin_context.models.app.PullStreamPluginContext
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, Task, ZIO, ZLayer}

import java.net.URI
import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}
import scala.jdk.CollectionConverters.*

object IntegrationTests extends ZIOSpecDefault:

  private val DynamoEndpoint   = sys.env.getOrElse("DYNAMODB_ENDPOINT", "http://localhost:8000")
  private val Region_          = "us-east-1"
  private val PrimaryKeyField  = "producer"
  private val PrimaryKeyValue  = "producer1"
  private val WatermarkField   = "timestampUTC"
  private val SourceTableShort = "stream_pull_test"
  private val TargetTableFull  = s"iceberg.test.$SourceTableShort"

  // Iceberg schema for both the source-schema lookup table and the merge target.
  // MergeKeyField is required so the merge processor can locate a merge key; TimestampUTC is used
  // by PullStreamChangeTrackingMergeBatch as the version field in its `ORDER BY ... DESC` clause.
  private val sourceSchema: ArcaneSchema = ArcaneSchema(
    Seq(
      Field("id", StringType),
      Field("value", StringType),
      Field("TimestampUTC", StringType),
      MergeKeyField
    )
  )

  private def buildDynamoClient: Task[DynamoDbClient] = ZIO.attempt(
    DynamoDbClient
      .builder()
      .endpointOverride(URI.create(DynamoEndpoint))
      .region(Region.of(Region_))
      .build()
  )

  private def createSourceTable(client: DynamoDbClient, tableName: String): Task[Unit] =
    ZIO
      .attemptBlocking {
        val req = CreateTableRequest
          .builder()
          .tableName(tableName)
          .keySchema(
            KeySchemaElement.builder().attributeName(PrimaryKeyField).keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName(WatermarkField).keyType(KeyType.RANGE).build()
          )
          .attributeDefinitions(
            AttributeDefinition.builder().attributeName(PrimaryKeyField).attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder().attributeName(WatermarkField).attributeType(ScalarAttributeType.S).build()
          )
          .provisionedThroughput(
            ProvisionedThroughput.builder().readCapacityUnits(5L).writeCapacityUnits(5L).build()
          )
          .build()
        client.createTable(req)
        ()
      }
      .catchSome { case _: ResourceInUseException => ZIO.unit }

  private def deleteSourceTable(client: DynamoDbClient, tableName: String): zio.UIO[Unit] =
    ZIO
      .attemptBlocking(client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build()))
      .unit
      .catchAll(_ => ZIO.unit)

  private def insertItem(
      client: DynamoDbClient,
      tableName: String,
      timestamp: OffsetDateTime,
      payloadJson: String
  ): Task[Unit] = ZIO.attemptBlocking {
    val item = Map(
      PrimaryKeyField -> AttributeValue.builder().s(PrimaryKeyValue).build(),
      WatermarkField  -> AttributeValue.builder().s(timestamp.toString).build(),
      "payload"       -> AttributeValue.builder().s(payloadJson).build()
    ).asJava
    client.putItem(PutItemRequest.builder().tableName(tableName).item(item).build())
    ()
  }

  private def streamContextJson(endpoint: String): String =
    s"""
       |{
       |  "backfillJobTemplateRef": {
       |    "apiGroup": "streaming.sneaksanddata.com",
       |    "kind": "StreamingJobTemplate",
       |    "name": "noop"
       |  },
       |  "jobTemplateRef": {
       |    "apiGroup": "streaming.sneaksanddata.com",
       |    "kind": "StreamingJobTemplate",
       |    "name": "noop"
       |  },
       |  "observability": {
       |    "metricTags": {}
       |  },
       |  "staging": {
       |    "table": {
       |      "maxRowsPerFile": 10000,
       |      "stagingCatalogName": "iceberg",
       |      "stagingSchemaName": "test",
       |      "isUnifiedSchema": false
       |    },
       |    "icebergCatalog": {
       |      "catalogProperties": {},
       |      "catalogUri": "http://localhost:20001/catalog",
       |      "namespace": "test",
       |      "warehouse": "demo",
       |      "maxCatalogInstanceLifetime": "3600 second"
       |    }
       |  },
       |  "streamMode": {
       |    "backfill": {
       |      "backfillBehavior": "Overwrite",
       |      "backfillStartDate": "2026-01-01T00:00:00Z"
       |    },
       |    "changeCapture": {
       |      "changeCaptureInterval": "2 second",
       |      "changeCaptureJitterVariance": 0.1,
       |      "changeCaptureJitterSeed": 0
       |    }
       |  },
       |  "sink": {
       |    "mergeServiceClient": {
       |      "connectionUrl": "jdbc:trino://localhost:8080",
       |      "credentialType": { "basic": {} },
       |      "extraConnectionParameters": { "clientTags": "test" },
       |      "queryRetryMode": { "never": {} },
       |      "queryRetryBaseDuration": "100 millisecond",
       |      "queryRetryOnMessageContents": [],
       |      "queryRetryScaleFactor": 0.1,
       |      "queryRetryMaxAttempts": 3
       |    },
       |    "targetTableProperties": {
       |      "format": "PARQUET",
       |      "sortedBy": [],
       |      "parquetBloomFilterColumns": []
       |    },
       |    "targetTableFullName": "$TargetTableFull",
       |    "maintenanceSettings": {
       |      "targetOptimizeSettings": { "batchThreshold": 60, "fileSizeThreshold": "512MB" },
       |      "targetOrphanFilesExpirationSettings": { "batchThreshold": 60, "retentionThreshold": "6h" },
       |      "targetSnapshotExpirationSettings": { "batchThreshold": 60, "retentionThreshold": "6h" },
       |      "targetAnalyzeSettings": { "includedColumns": [], "batchThreshold": 60 }
       |    },
       |    "icebergCatalog": {
       |      "catalogProperties": {},
       |      "catalogUri": "http://localhost:20001/catalog",
       |      "namespace": "test",
       |      "warehouse": "demo",
       |      "maxCatalogInstanceLifetime": "3600 second"
       |    }
       |  },
       |  "throughput": {
       |    "shaperImpl": {
       |      "memoryBound": {
       |        "fallbackStringTypeSizeEstimate": 50,
       |        "objectTypeSizeEstimate": 4096,
       |        "chunkCostScale": 1,
       |        "chunkCostMax": 10,
       |        "tableRowCountWeight": 0.05,
       |        "tableSizeWeight": 0.05,
       |        "tableSizeScaleFactor": 1
       |      }
       |    },
       |    "advisedRate": "1000 per 1 second",
       |    "advisedBurst": 1000,
       |    "advisedChunkSize": 10
       |  },
       |  "source": {
       |    "configuration": {
       |      "pullIndexKey": "$PrimaryKeyField",
       |      "pullIndexValue": "$PrimaryKeyValue",
       |      "watermarkFieldName": "$WatermarkField",
       |      "region": "$Region_",
       |      "tableName": "$SourceTableShort",
       |      "endpoint": "$endpoint"
       |    },
       |    "buffering": {
       |      "enabled": false,
       |      "strategy": {}
       |    },
       |    "fieldSelectionRule": {
       |      "essentialFields": [],
       |      "rule": { "all": {} },
       |      "isServerSide": false
       |    }
       |  }
       |}""".stripMargin

  private def payloadFor(items: Seq[(String, String, OffsetDateTime, String)]): String =
    items
      .map { case (mergeKey, id, ts, value) =>
        s"""{"id":"$id","value":"$value","TimestampUTC":"${ts.toString}","ARCANE_MERGE_KEY":"$mergeKey"}"""
      }
      .mkString("[", ",", "]")

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("PullStreamIntegrationTests")(
    test("streams rows from DynamoDB into the parquet target table") {
      ZIO.scoped {
        for {
          client <- buildDynamoClient.withFinalizer(c => ZIO.attempt(c.close()).orDie)
          _ <- ZIO.acquireRelease(createSourceTable(client, SourceTableShort))(_ =>
            deleteSourceTable(client, SourceTableShort)
          )

          _ <- ZIO.attempt(clearTarget(TargetTableFull))

          // The pull source uses `comment` on the target iceberg table as its watermark.
          // We set it to "now()" so any item we insert with a strictly greater timestamp will be ingested.
          watermarkStart = PullStreamWatermark(
            OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC).minusSeconds(1)
          )
          _ <- prepareWatermark(SourceTableShort, sourceSchema, watermarkStart)

          // Seed three rows with strictly increasing timestamps so the source advances its watermark.
          now = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC).plusSeconds(5)
          _ <- insertItem(
            client,
            SourceTableShort,
            now,
            payloadFor(
              Seq(
                ("k1", "1", now.plusSeconds(1), "value-1"),
                ("k2", "2", now.plusSeconds(2), "value-2"),
                ("k3", "3", now.plusSeconds(3), "value-3")
              )
            )
          )

          contextLayer: ZLayer[Any, Nothing, com.sneaksanddata.arcane.framework.models.app.PluginStreamContext] =
            ZLayer.succeed(PullStreamPluginContext(streamContextJson(DynamoEndpoint)))

          runner <- Common.getTestApp(Duration.ofSeconds(15), contextLayer).fork
          _      <- runner.runOrFail(zio.Duration.fromSeconds(20))

          rows <- readTarget(
            TargetTableFull,
            "arcane_merge_key, id, value, timestamputc",
            Common.TargetDecoder
          )
        } yield assertTrue(rows.size == 3) &&
          assertTrue(rows.map(_._1).toSet == Set("k1", "k2", "k3")) &&
          assertTrue(rows.map(_._3).toSet == Set("value-1", "value-2", "value-3"))
      }
    }
  ) @@ timeout(zio.Duration.fromSeconds(30))
    @@ TestAspect.withLiveClock
    @@ TestAspect.sequential
    @@ TestAspect.before(liveSeed)

package com.sneaksanddata.arcane.stream_pull
package tests

import com.sneaksanddata.arcane.framework.migrations.base.CurrentMigration
import com.sneaksanddata.arcane.framework.models.MetadataKeys
import com.sneaksanddata.arcane.framework.models.schemas.ArcaneType.StringType
import com.sneaksanddata.arcane.framework.models.schemas.{
  ArcaneSchema,
  Field,
  MergeKeyField,
  VersionField as ArcaneVersionField
}
import com.sneaksanddata.arcane.framework.services.pullstream.PullStreamingSource
import com.sneaksanddata.arcane.framework.services.pullstream.versioning.PullStreamWatermark
import com.sneaksanddata.arcane.framework.testkit.iceberg.TestPropertyManager
import com.sneaksanddata.arcane.framework.testkit.setups.FrameworkTestSetup.prepareWatermark
import com.sneaksanddata.arcane.framework.testkit.verifications.FrameworkVerificationUtilities.{clearTarget, readTarget}
import com.sneaksanddata.arcane.framework.testkit.zioutils.ZKit.{liveSeed, runOrFail}
import com.sneaksanddata.arcane.framework.utils.HashUtils
import com.sneaksanddata.arcane.pull_stream_plugin_context.models.app.PullStreamPluginContext
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import zio.test.*
import zio.test.TestAspect.timeout
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig
import zio.{Scope, Task, ZIO, ZLayer}

import java.net.URI
import java.sql.Connection
import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}
import scala.jdk.CollectionConverters.*

object IntegrationTests extends ZIOSpecDefault:

  private val DynamoEndpoint   = sys.env.getOrElse("DYNAMODB_ENDPOINT", "http://localhost:8000")
  private val Region_          = "us-east-1"
  private val PrimaryKeyField  = "producer"
  private val PrimaryKeyValue  = "producer1"
  private val VersionField     = "timestampUTC"
  private val SourceTableShort = "stream_pull_test"
  private val TargetTableFull  = s"iceberg.test.$SourceTableShort"

  private val MigrationKey = MetadataKeys.getFrameworkKey(MetadataKeys.migrationKey)

  private val MigrationTargetShort = s"${SourceTableShort}_migration"
  private val MigrationTargetFull  = s"iceberg.test.$MigrationTargetShort"
  private val TrinoConnectionUri = sys.env.getOrElse(
    "ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI",
    "jdbc:trino://localhost:8080/iceberg/test?user=test"
  )

  // sink table layout produced before ARCANE_MERGE_KEY was hashed and before ARCANE_VERSION replaced the
  // literal version column (`TimestampUTC`)
  private val legacySchema: ArcaneSchema = ArcaneSchema(
    Seq(
      Field("id", StringType),
      Field("value", StringType),
      Field("TimestampUTC", StringType),
      ArcaneVersionField,
      MergeKeyField
    )
  )

  // (merge key, id, value, legacy version)
  private val legacyRows = Seq(
    ("legacy-1", "1", "old-1", "2026-01-01T10:00:00Z"),
    ("legacy-2", "2", "old-2", "2026-01-01T11:00:00Z")
  )
  private def epochMillis(iso: String): String = java.time.Instant.parse(iso).toEpochMilli.toString

  // sbt's layered classloaders hide the JDBC driver from DriverManager's ServiceLoader lookup
  private val registerDriver = ZIO.attempt(java.sql.DriverManager.registerDriver(new io.trino.jdbc.TrinoDriver()))

  // the source layer refuses to start unless the sink table carries these plugin properties
  private def setPluginProperties(tableShortName: String, markMigrated: Boolean): Task[Unit] = ZIO.scoped {
    for
      manager <- TestPropertyManager.sinkPropertyManager
      _       <- manager.setProperty(tableShortName, PullStreamingSource.jsonPointerPropertyKey, "")
      _       <- manager.setProperty(tableShortName, PullStreamingSource.pageSizePropertyKey, "1000")
      _       <- manager.setProperty(tableShortName, PullStreamingSource.decodeChunkSizePropertyKey, "32")
      _       <- ZIO.when(markMigrated)(manager.setProperty(tableShortName, MigrationKey, CurrentMigration.toString))
    yield ()
  }

  private def connect(): Connection = java.sql.DriverManager.getConnection(TrinoConnectionUri)

  private def seedLegacyRows(table: String): Task[Unit] = ZIO.scoped {
    for
      connection <- ZIO.attempt(connect())
      _          <- ZIO.addFinalizer(ZIO.attempt(connection.close()).orDie)
      statement  <- ZIO.attempt(connection.createStatement())
      values = legacyRows
        .map { case (key, id, value, version) => s"('$id', '$value', '$version', '$key')" }
        .mkString(", ")
      _ <- ZIO.attemptBlocking(
        statement.execute(s"INSERT INTO $table (id, value, TimestampUTC, ARCANE_MERGE_KEY) VALUES $values")
      )
    yield ()
  }

  private def readColumns(table: String): Task[List[String]] = ZIO.scoped {
    for
      connection <- ZIO.attempt(connect())
      _          <- ZIO.addFinalizer(ZIO.attempt(connection.close()).orDie)
      statement  <- ZIO.attempt(connection.createStatement())
      resultSet  <- ZIO.attemptBlocking(statement.executeQuery(s"DESCRIBE $table"))
      columns <- ZIO.attempt(Iterator.continually(resultSet).takeWhile(_.next()).map(_.getString(1).toLowerCase).toList)
    yield columns
  }

  private def readProperty(table: String, key: String): Task[Option[String]] = ZIO.scoped {
    for
      connection <- ZIO.attempt(connect())
      _          <- ZIO.addFinalizer(ZIO.attempt(connection.close()).orDie)
      statement  <- ZIO.attempt(connection.createStatement())
      resultSet <- ZIO.attemptBlocking(
        statement.executeQuery(
          s"""SELECT value FROM iceberg.test."${table.split('.').last}$$properties" WHERE key = '$key'"""
        )
      )
      value <- ZIO.attempt(if resultSet.next() then Some(resultSet.getString(1)) else None)
    yield value
  }

  // Iceberg schema for both the source-schema lookup table and the merge target.
  // MergeKeyField is required so the merge processor can locate a merge key; ARCANE_VERSION carries the DynamoDB
  // watermark and orders concurrent versions during a merge.
  private val sourceSchema: ArcaneSchema = ArcaneSchema(
    Seq(
      Field("id", StringType),
      Field("value", StringType),
      ArcaneVersionField,
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
            KeySchemaElement.builder().attributeName(VersionField).keyType(KeyType.RANGE).build()
          )
          .attributeDefinitions(
            AttributeDefinition.builder().attributeName(PrimaryKeyField).attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder().attributeName(VersionField).attributeType(ScalarAttributeType.S).build()
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
      mergeKey: String,
      payloadJson: String
  ): Task[Unit] = ZIO.attemptBlocking {
    val item = Map(
      PrimaryKeyField -> AttributeValue.builder().s(PrimaryKeyValue).build(),
      VersionField    -> AttributeValue.builder().s(timestamp.toString).build(),
      // the source takes the merge key from the item's `id` attribute, exactly as arcane-push-stream
      // writes it: one pushed message identifies one target row, and the payload carries no identity
      // of its own. Without it every row merges under a null key and they collapse into one.
      "id"      -> AttributeValue.builder().s(mergeKey).build(),
      "payload" -> AttributeValue.builder().s(payloadJson).build()
    ).asJava
    client.putItem(PutItemRequest.builder().tableName(tableName).item(item).build())
    ()
  }

  private def streamContextJson(endpoint: String, targetTable: String = TargetTableFull): String =
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
       |      "warehouse": "demo"
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
       |      "changeCaptureJitterSeed": 0,
       |      "changeCaptureRangeLimit": 100
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
       |    "targetTableFullName": "$targetTable",
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
       |      "warehouse": "demo"
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
       |        "tableSizeScaleFactor": 1,
       |        "chunkSizeCap": 1000000,
       |        "maxStatisticsAge": 604800
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
       |      "versionFieldName": "$VersionField",
       |      "region": "$Region_",
       |      "tableName": "$SourceTableShort",
       |      "endpoint": "$endpoint"
       |    },
       |    "buffering": {
       |      "enabled": false,
       |      "strategy": {}
       |    },
       |    "modifications": {"supported": {}}
       |  }
       |}""".stripMargin

  /** Body of one pushed message, in the shape arcane-push-stream persists: business fields only.
    *
    * The merge key and the version are deliberately absent — the source prunes those columns from the decode schema and
    * fills them from the DynamoDB item's own attributes, so a copy inside the payload would be ignored.
    */
  private def payloadFor(id: String, value: String): String =
    s"""{"id":"$id","value":"$value"}"""

  private def contextLayer(context: PullStreamPluginContext) =
    ZLayer.succeed[com.sneaksanddata.arcane.framework.models.app.PluginStreamContext](context) ++
      ZLayer.succeed[DatagramSocketConfig](context) ++ ZLayer.succeed[MetricsConfig](context) ++
      ZLayer.succeed(DatadogPublisherConfig())

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("PullStreamIntegrationTests")(
    test("migrates a legacy sink table when run in migrate mode") {
      ZIO.scoped {
        for
          client <- buildDynamoClient.withFinalizer(c => ZIO.attempt(c.close()).orDie)
          _ <- ZIO.acquireRelease(createSourceTable(client, SourceTableShort))(_ =>
            deleteSourceTable(client, SourceTableShort)
          )
          _ <- clearTarget(MigrationTargetFull)
          _ <- prepareWatermark(
            MigrationTargetShort,
            legacySchema,
            PullStreamWatermark(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC))
          )
          _ <- setPluginProperties(MigrationTargetShort, markMigrated = false)
          _ <- seedLegacyRows(MigrationTargetFull)

          columnsBefore   <- readColumns(MigrationTargetFull)
          migrationBefore <- readProperty(MigrationTargetFull, MigrationKey)

          context = PullStreamPluginContext(streamContextJson(DynamoEndpoint, MigrationTargetFull))
          _        <- TestSystem.putEnv("STREAMCONTEXT__RUNMODE", "migrate")
          migrator <- Common.getTestApp(Duration.ofSeconds(45), contextLayer(context)).fork
          _        <- migrator.runOrFail(zio.Duration.fromSeconds(40))
          _        <- TestSystem.putEnv("STREAMCONTEXT__RUNMODE", "stream")

          columnsAfter   <- readColumns(MigrationTargetFull)
          migrationAfter <- readProperty(MigrationTargetFull, MigrationKey)
          pageSize       <- readProperty(MigrationTargetFull, PullStreamingSource.pageSizePropertyKey)
          rows <- readTarget(
            MigrationTargetFull,
            "ARCANE_MERGE_KEY, ARCANE_VERSION",
            (rs: java.sql.ResultSet) => (rs.getString(1), rs.getString(2))
          )
        yield assertTrue(columnsBefore.contains("timestamputc")) &&
          assertTrue(migrationBefore.isEmpty) &&
          assertTrue(!columnsAfter.contains("timestamputc")) &&
          assertTrue(columnsAfter.contains("arcane_merge_key") && columnsAfter.contains("arcane_version")) &&
          assertTrue(migrationAfter.contains(CurrentMigration.toString)) &&
          assertTrue(pageSize.contains("1000")) &&
          assertTrue(
            rows.sorted == legacyRows.map { case (key, _, _, version) =>
              (HashUtils.murmur3(key), epochMillis(version))
            }.sorted
          )
      }
    },
    test("streams rows from DynamoDB into the parquet target table") {
      ZIO.scoped {
        for {
          client <- buildDynamoClient.withFinalizer(c => ZIO.attempt(c.close()).orDie)
          _ <- ZIO.acquireRelease(createSourceTable(client, SourceTableShort))(_ =>
            deleteSourceTable(client, SourceTableShort)
          )

          _ <- ZIO.attempt(clearTarget(TargetTableFull))

          // The pull source uses `comment` on the target iceberg table as its version.
          // We set it to "now()" so any item we insert with a strictly greater timestamp will be ingested.
          watermarkStart = PullStreamWatermark(
            OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC).minusSeconds(1)
          )
          _ <- prepareWatermark(SourceTableShort, sourceSchema, watermarkStart)
          _ <- setPluginProperties(SourceTableShort, markMigrated = true)

          // Seed three messages with strictly increasing timestamps so the source advances its version.
          // One item per row, as in production: the item's `id` attribute is what identifies the target row.
          now = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC).plusSeconds(5)
          _ <- ZIO.foreachDiscard(
            Seq(("k1", "1", "value-1"), ("k2", "2", "value-2"), ("k3", "3", "value-3")).zipWithIndex
          ) { case ((mergeKey, id, value), index) =>
            insertItem(
              client,
              SourceTableShort,
              now.plusSeconds(index.toLong + 1),
              mergeKey,
              payloadFor(id, value)
            )
          }

          streamContext = PullStreamPluginContext(streamContextJson(DynamoEndpoint))

          runner <- Common.getTestApp(Duration.ofSeconds(15), contextLayer(streamContext)).fork
          _      <- runner.runOrFail(zio.Duration.fromSeconds(20))

          rows <- readTarget(
            TargetTableFull,
            "arcane_merge_key, id, value, arcane_version",
            Common.TargetDecoder
          )
        } yield assertTrue(rows.size == 3) &&
          assertTrue(rows.map(_._1).toSet == Set("k1", "k2", "k3").map(HashUtils.murmur3)) &&
          assertTrue(rows.map(_._3).toSet == Set("value-1", "value-2", "value-3"))
      }
    }
  ) @@ timeout(zio.Duration.fromSeconds(120))
    @@ TestAspect.withLiveClock
    @@ TestAspect.sequential
    @@ TestAspect.before(liveSeed *> registerDriver)

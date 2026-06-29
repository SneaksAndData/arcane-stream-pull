package com.sneaksanddata.arcane.stream_dynamodb
package tests

import main.{appLayer, blobSourceLayer, s3ReaderLayer}
import models.app.DynamodbPluginStreamContext

import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, StreamGraphResolver}
import com.sneaksanddata.arcane.framework.services.backfill.DefaultBackfillStateManager
import com.sneaksanddata.arcane.framework.services.backfill.processors.{
  BackfillCompletionProcessor,
  ShardStagingProcessor
}
import com.sneaksanddata.arcane.framework.services.blobsource.backfill.{
  BlobBackfillSourceDataProvider,
  BlobShardedBackfillStreamDataProvider,
  BlobSourceBackfillMergeStreamDataProvider,
  BlobSourceShardFactory
}
import com.sneaksanddata.arcane.framework.services.blobsource.providers.{
  BlobSourceDataProvider,
  BlobSourceStreamingDataProvider
}
import com.sneaksanddata.arcane.framework.services.blobsource.readers.listing.BlobListingJsonSource
import com.sneaksanddata.arcane.framework.services.blobsource.versioning.UpsertBlobStagedBatchFactory
import com.sneaksanddata.arcane.framework.services.bootstrap.DefaultStreamBootstrapper
import com.sneaksanddata.arcane.framework.services.filters.FieldsFilteringService
import com.sneaksanddata.arcane.framework.services.iceberg.{
  IcebergEntityManager,
  IcebergS3CatalogWriter,
  IcebergTablePropertyManager
}
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.merging.cleanup.CatalogDisposeServiceClient
import com.sneaksanddata.arcane.framework.services.metrics.{DeclaredMetrics, GlobalMetricTagProvider}
import com.sneaksanddata.arcane.framework.services.naming.DefaultNameGenerator
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.maintenance.TargetMaintenanceProcessor
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  SchemaMigrationProcessor,
  WatermarkProcessor
}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.{
  FieldFilteringTransformer,
  StagingProcessor
}
import com.sneaksanddata.arcane.framework.services.streaming.throughput.base.ThroughputShaperBuilder
import com.sneaksanddata.arcane.framework.testkit.appbuilder.TestAppBuilder.buildTestApp
import com.sneaksanddata.arcane.framework.testkit.streaming.TimeLimitLifetimeService
import zio.{ULayer, ZIO, ZLayer}

import java.sql.ResultSet
import java.time.Duration

/** Common utilities for tests.
  */
object Common:

  /** Builds the test application from the provided layers.
    * @param streamContextLayer
    *   The stream context layer.
    * @return
    *   The test application.
    */
  def getTestApp(
      runDuration: Duration,
      streamContextLayer: ZLayer[Any, Nothing, DynamodbPluginStreamContext]
  ): ZIO[Any, Throwable, Unit] =
    buildTestApp(
      appLayer,
      streamContextLayer,
      s3ReaderLayer,
      BlobSourceStreamingDataProvider.layer
    )(
      GenericStreamRunnerService.layer,
      StreamGraphResolver.composedLayer,
      DisposeBatchProcessor.layer,
      FieldFilteringTransformer.layer,
      MergeBatchProcessor.layer,
      StagingProcessor.layer,
      FieldsFilteringService.layer,
      IcebergS3CatalogWriter.layer,
      JdbcMergeServiceClient.layer,
      DeclaredMetrics.layer,
      GlobalMetricTagProvider.layer,
      WatermarkProcessor.layer,
      ZLayer.succeed(TimeLimitLifetimeService(runDuration)),
      BlobSourceDataProvider.layer,
      blobSourceLayer,
      UpsertBlobStagedBatchFactory.layer,

      // backfill
      BlobBackfillSourceDataProvider.layer,
      BlobSourceShardFactory.layer,
      BlobShardedBackfillStreamDataProvider.layer,
      BlobSourceBackfillMergeStreamDataProvider.layer,
      DefaultBackfillStateManager.layer,
      ShardStagingProcessor.layer,
      BackfillCompletionProcessor.layer,

      // schema
      SchemaMigrationProcessor.layer,

      // maintenance and cleanup
      TargetMaintenanceProcessor.layer,
      CatalogDisposeServiceClient.layer,
      DefaultNameGenerator.layer,
      DefaultStreamBootstrapper.layer,
      ThroughputShaperBuilder.layer,
      IcebergEntityManager.sinkLayer,
      IcebergEntityManager.stagingLayer,
      IcebergTablePropertyManager.stagingLayer,
      IcebergTablePropertyManager.sinkLayer
    )

  val TargetDecoder: ResultSet => (Long, String, Long, String, Long, String, Long, String, Long, String, String, Long) =
    (rs: ResultSet) =>
      (
        rs.getLong(1),
        rs.getString(2),
        rs.getLong(3),
        rs.getString(4),
        rs.getLong(5),
        rs.getString(6),
        rs.getLong(7),
        rs.getString(8),
        rs.getLong(9),
        rs.getString(10),
        rs.getString(11),
        rs.getLong(12)
      )

  val TargetNestedDecoder: ResultSet => (
      Long,
      String,
      Long,
      String,
      Long,
      String,
      Long,
      String,
      Long,
      String,
      String,
      Long,
      String,
      Long
  ) =
    (rs: ResultSet) =>
      (
        rs.getLong(1),
        rs.getString(2),
        rs.getLong(3),
        rs.getString(4),
        rs.getLong(5),
        rs.getString(6),
        rs.getLong(7),
        rs.getString(8),
        rs.getLong(9),
        rs.getString(10),
        rs.getString(11),
        rs.getLong(12),
        rs.getString(13),
        rs.getLong(14)
      )

  val avroSchemaString =
    """{ \"name\": \"GeneratedAvroSchemaTest\", \"namespace\": \"com.group.GeneratedAvroSchemaTest\", \"doc\": \"Unit test data schema\", \"type\": \"record\", \"fields\": [ { \"name\": \"col0\", \"type\": [ \"null\", \"int\" ], \"default\": null }, { \"name\": \"col1\", \"type\": [ \"null\", \"string\" ], \"default\": null }, { \"name\": \"col2\", \"type\": [ \"null\", \"int\" ], \"default\": null }, { \"name\": \"col3\", \"type\": [ \"null\", \"string\" ], \"default\": null }, { \"name\": \"col4\", \"type\": [ \"null\", \"int\" ], \"default\": null }, { \"name\": \"col5\", \"type\": [ \"null\", \"string\" ], \"default\": null }, { \"name\": \"col6\", \"type\": [ \"null\", \"int\" ], \"default\": null }, { \"name\": \"col7\", \"type\": [ \"null\", \"string\" ], \"default\": null }, { \"name\": \"col8\", \"type\": [ \"null\", \"int\" ], \"default\": null }, { \"name\": \"col9\", \"type\": [ \"null\", \"string\" ], \"default\": null } ] }"""
  val nestedAvroSchemaString =
    """{ \"name\": \"BlobListingJsonSource\", \"namespace\": \"com.sneaksanddata.arcane.BlobListingJsonSource\", \"doc\": \"Avro Schema with nested fields for BlobListingJsonSource tests\", \"type\": \"record\", \"fields\": [ { \"name\": \"col0\", \"type\": [ \"null\", \"int\" ], \"default\": null }, { \"name\": \"col1\", \"type\": [ \"null\", \"string\" ], \"default\": null }, { \"name\": \"col2\", \"type\": [ \"null\", \"int\" ], \"default\": null }, { \"name\": \"col3\", \"type\": [ \"null\", \"string\" ], \"default\": null }, { \"name\": \"col4\", \"type\": [ \"null\", \"int\" ], \"default\": null }, { \"name\": \"col5\", \"type\": [ \"null\", \"string\" ], \"default\": null }, { \"name\": \"col6\", \"type\": [ \"null\", \"int\" ], \"default\": null }, { \"name\": \"col7\", \"type\": [ \"null\", \"string\" ], \"default\": null }, { \"name\": \"col8\", \"type\": [ \"null\", \"int\" ], \"default\": null }, { \"name\": \"col9\", \"type\": [ \"null\", \"string\" ], \"default\": null }, { \"name\": \"nested_col_1\", \"type\": [ \"null\", \"string\" ], \"default\": null }, { \"name\": \"nested_col_2\", \"type\": [ \"null\", \"int\" ], \"default\": null } ] }"""

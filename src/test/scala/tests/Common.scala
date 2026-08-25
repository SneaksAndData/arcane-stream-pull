package com.sneaksanddata.arcane.stream_pull
package tests

import main.{appLayer, dynamoDbClientLayer, pullStreamingSourceLayer}

import com.sneaksanddata.arcane.framework.models.app.PluginStreamContext
import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, StreamGraphResolver}
import com.sneaksanddata.arcane.framework.services.backfill.DefaultBackfillStateManager
import com.sneaksanddata.arcane.framework.services.backfill.processors.{
  BackfillCompletionProcessor,
  ShardStagingProcessor
}
import com.sneaksanddata.arcane.framework.services.bootstrap.DefaultStreamBootstrapper
import com.sneaksanddata.arcane.framework.services.completion.DefaultStreamFinalizer
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
import com.sneaksanddata.arcane.framework.services.pullstream.{
  PullStreamSourceDataProvider,
  PullStreamStagedBatchFactory,
  PullStreamStreamingDataProvider
}
import com.sneaksanddata.arcane.framework.services.pullstream.backfill.{
  NoopBackfillStreamDataProvider,
  NoopShardFactory,
  NoopShardedBackfillStreamDataProvider
}
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
import zio.{ZIO, ZLayer}

import java.sql.ResultSet
import java.time.Duration

/** Common utilities for tests.
  */
object Common:

  /** Builds the test application from the provided layers.
    *
    * @param runDuration
    *   How long the streaming application should run before being interrupted.
    * @param streamContextLayer
    *   The stream context layer providing a [[PluginStreamContext]] backed by a [[PullStreamPluginContext]].
    */
  def getTestApp(
      runDuration: Duration,
      streamContextLayer: ZLayer[Any, Nothing, PluginStreamContext]
  ): ZIO[Any, Throwable, Unit] =
    buildTestApp(
      appLayer,
      streamContextLayer,
      pullStreamingSourceLayer,
      ZLayer.succeed(TimeLimitLifetimeService(runDuration))
    )(
      dynamoDbClientLayer,
      GenericStreamRunnerService.layer,
      StreamGraphResolver.composedLayer,
      DisposeBatchProcessor.layer,
      FieldFilteringTransformer.layer,
      MergeBatchProcessor.layer,
      StagingProcessor.layer,
      FieldsFilteringService.layer,
      SchemaMigrationProcessor.layer,

      // pullStreamPlugin layers
      PullStreamStagedBatchFactory.layer,
      PullStreamSourceDataProvider.layer,
      PullStreamStreamingDataProvider.layer,
      DefaultBackfillStateManager.layer,
      ShardStagingProcessor.layer,
      BackfillCompletionProcessor.layer,
      NoopBackfillStreamDataProvider.layer,
      NoopShardedBackfillStreamDataProvider.layer,
      NoopShardFactory.layer,

      // sink / staging
      IcebergS3CatalogWriter.layer,
      IcebergEntityManager.sinkLayer,
      IcebergEntityManager.stagingLayer,
      IcebergTablePropertyManager.stagingLayer,
      IcebergTablePropertyManager.sinkLayer,
      JdbcMergeServiceClient.layer,

      // observability (no DataDog publisher in tests)
      DeclaredMetrics.layer,
      GlobalMetricTagProvider.layer,
      WatermarkProcessor.layer,

      // maintenance and cleanup
      TargetMaintenanceProcessor.layer,
      CatalogDisposeServiceClient.layer,
      DefaultNameGenerator.layer,
      DefaultStreamBootstrapper.layer,
      DefaultStreamFinalizer.layer,
      ThroughputShaperBuilder.layer
    )

  val TargetDecoder: ResultSet => (String, String, String, String) =
    (rs: ResultSet) =>
      (
        rs.getString(1),
        rs.getString(2),
        rs.getString(3),
        rs.getString(4)
      )

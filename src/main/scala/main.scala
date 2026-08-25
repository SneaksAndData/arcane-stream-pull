package com.sneaksanddata.arcane.stream_pull

import com.sneaksanddata.arcane.framework.extensions.ZExtensions.*
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.app.PluginStreamContext
import com.sneaksanddata.arcane.framework.services.app.base.StreamRunnerService
import com.sneaksanddata.arcane.framework.services.app.{
  GenericStreamRunnerService,
  PosixStreamLifetimeService,
  StreamGraphResolver
}
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
import com.sneaksanddata.arcane.framework.services.metrics.{DataDog, DeclaredMetrics, GlobalMetricTagProvider}
import com.sneaksanddata.arcane.framework.services.naming.DefaultNameGenerator
import com.sneaksanddata.arcane.framework.services.pullstream.*
import com.sneaksanddata.arcane.framework.services.pullstream.PullStreamingSource
import com.sneaksanddata.arcane.framework.services.storage.models.s3.S3StoragePath
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
import com.sneaksanddata.arcane.framework.services.pullstream.backfill.{
  NoopBackfillStreamDataProvider,
  NoopShardedBackfillStreamDataProvider,
  NoopShardFactory
}
import com.sneaksanddata.arcane.pull_stream_plugin_context.models.app.PullStreamPluginContext
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import zio.*
import zio.logging.backend.SLF4J

import java.net.URI

object main extends ZIOAppDefault {

  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  val appLayer: ZIO[StreamRunnerService, Throwable, Unit] = for
    _            <- zlog("Application starting")
    streamRunner <- ZIO.service[StreamRunnerService]
    _            <- streamRunner.run
  yield ()

  val pullStreamingSourceLayer =
    PullStreamingSource.getLayer(context => context.asInstanceOf[PullStreamPluginContext].source.configuration)

  val dynamoDbClientLayer: ZLayer[PluginStreamContext, Throwable, DynamoDbClient] =
    ZLayer.scoped {
      ZIO.fromAutoCloseable {
        for
          context <- ZIO.service[PluginStreamContext]
          settings = context.asInstanceOf[PullStreamPluginContext].source.configuration
          client <- ZIO.attempt {
            val builder = DynamoDbClient.builder().region(Region.of(settings.region))
            settings.endpoint.foreach(ep => builder.endpointOverride(URI.create(ep)))
            builder.build()
          }
        yield client
      }
    }

  private lazy val streamRunner = appLayer.provide(
    GenericStreamRunnerService.layer,
    StreamGraphResolver.composedLayer,
    DisposeBatchProcessor.layer,
    FieldFilteringTransformer.layer,
    MergeBatchProcessor.layer,
    StagingProcessor.layer,
    FieldsFilteringService.layer,
    PosixStreamLifetimeService.layer,

    // schema
    SchemaMigrationProcessor.layer,
    // pullStreamPlugin
    PullStreamStagedBatchFactory.layer,
    PullStreamSourceDataProvider.layer,
    PullStreamStreamingDataProvider.layer,
    DefaultBackfillStateManager.layer,
    ShardStagingProcessor.layer,
    BackfillCompletionProcessor.layer,
    NoopBackfillStreamDataProvider.layer,
    NoopShardedBackfillStreamDataProvider.layer,
    NoopShardFactory.layer,
    pullStreamingSourceLayer,
    PullStreamPluginContext.layer,
    dynamoDbClientLayer,

    // maintenance and cleanup
    TargetMaintenanceProcessor.layer,
    CatalogDisposeServiceClient.layer,
    DefaultNameGenerator.layer,
    IcebergS3CatalogWriter.layer,
    IcebergEntityManager.sinkLayer,
    IcebergEntityManager.stagingLayer,
    IcebergTablePropertyManager.stagingLayer,
    IcebergTablePropertyManager.sinkLayer,
    JdbcMergeServiceClient.layer,
    DeclaredMetrics.layer,
    GlobalMetricTagProvider.layer,
    DataDog.UdsPublisher.layer,
    WatermarkProcessor.layer,
    DefaultStreamBootstrapper.layer,
    DefaultStreamFinalizer.layer,
    ThroughputShaperBuilder.layer
  )

  @main
  def run: ZIO[Any, Throwable, Unit] = streamRunner.handleAppFailure(exit)
}

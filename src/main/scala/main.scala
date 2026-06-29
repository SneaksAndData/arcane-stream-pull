package com.sneaksanddata.arcane.stream_pull

import com.sneaksanddata.arcane.framework.exceptions.StreamFailException
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.app.PluginStreamContext
import com.sneaksanddata.arcane.framework.services.app.base.StreamRunnerService
import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, PosixStreamLifetimeService, StreamGraphResolver}
import com.sneaksanddata.arcane.framework.services.backfill.DefaultBackfillStateManager
import com.sneaksanddata.arcane.framework.services.backfill.processors.{BackfillCompletionProcessor, ShardStagingProcessor}
import com.sneaksanddata.arcane.framework.services.blobsource.DefaultS3Reader
import com.sneaksanddata.arcane.framework.services.bootstrap.DefaultStreamBootstrapper
import com.sneaksanddata.arcane.framework.services.filters.FieldsFilteringService
import com.sneaksanddata.arcane.framework.services.iceberg.{IcebergEntityManager, IcebergS3CatalogWriter, IcebergTablePropertyManager}
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.merging.cleanup.CatalogDisposeServiceClient
import com.sneaksanddata.arcane.framework.services.metrics.{DataDog, DeclaredMetrics, GlobalMetricTagProvider}
import com.sneaksanddata.arcane.framework.services.naming.DefaultNameGenerator
import com.sneaksanddata.arcane.framework.services.pushstream.*
import com.sneaksanddata.arcane.framework.services.pushstream.PushStreamingSource
import com.sneaksanddata.arcane.framework.services.storage.models.s3.S3StoragePath
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.maintenance.TargetMaintenanceProcessor
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.streaming.{DisposeBatchProcessor, MergeBatchProcessor, SchemaMigrationProcessor, WatermarkProcessor}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import com.sneaksanddata.arcane.framework.services.streaming.throughput.base.ThroughputShaperBuilder
import com.sneaksanddata.arcane.framework.services.pushstream.backfill.{NoopBackfillStreamDataProvider, NoopShardedBackfillStreamDataProvider, NoopShardFactory}
import com.sneaksanddata.arcane.pull_stream_plugin_context.models.app.PullStreamPluginContext
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import zio.*
import zio.logging.backend.SLF4J

object main extends ZIOAppDefault {

  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  val appLayer: ZIO[StreamRunnerService, Throwable, Unit] = for
    _            <- zlog("Application starting")
    streamRunner <- ZIO.service[StreamRunnerService]
    _            <- streamRunner.run
  yield ()

  val pullStreamingSourceLayer =
    PushStreamingSource.getLayer(context =>
      context.asInstanceOf[PullStreamPluginContext].source.configuration
    )

  val dynamoDbClientLayer: ZLayer[PluginStreamContext, Throwable, DynamoDbClient] =
    ZLayer.scoped {
      ZIO.fromAutoCloseable(ZIO.attempt {
        val builder = software.amazon.awssdk.services.dynamodb.DynamoDbClient.builder()
          builder.build()
      })
    }

  private def getExitCode(exception: Throwable): zio.ExitCode =
    exception match
      case _: StreamFailException => zio.ExitCode(2)
      case _                      => zio.ExitCode(1)

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
    PushStreamStagedBatchFactory.layer,
    PushStreamSourceDataProvider.layer,
    PushStreamStreamingDataProvider.layer,
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
    ThroughputShaperBuilder.layer
  )

  @main
  def run: ZIO[Any, Throwable, Unit] =
    val app = streamRunner

    app.catchAllCause { cause =>
      for {
        _ <- zlog(s"Application failed: ${cause.squashTrace.getMessage}", cause)
        _ <- exit(getExitCode(cause.squashTrace))
      } yield ()
    }
}

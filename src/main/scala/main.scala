package com.sneaksanddata.arcane.stream_pull

import com.sneaksanddata.arcane.framework.extensions.ZExtensions.*
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.app.PluginStreamContext
import com.sneaksanddata.arcane.framework.models.schemas.ArcaneSchema
import com.sneaksanddata.arcane.framework.services.base.SchemaProvider
import com.sneaksanddata.arcane.framework.plugins.LayerAssemblies
import com.sneaksanddata.arcane.framework.plugins.pullstream.Services
import com.sneaksanddata.arcane.framework.services.app.base.StreamRunnerService
import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, StreamGraphResolver}
import com.sneaksanddata.arcane.framework.services.iceberg.IcebergTablePropertyManager
import com.sneaksanddata.arcane.framework.services.pullstream.PullStreamingSource
import com.sneaksanddata.arcane.pull_stream_plugin_context.models.app.PullStreamPluginContext
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import zio.*
import zio.logging.backend.SLF4J

import java.net.URI

object main extends ZIOAppDefault:

  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  val appLayer: ZIO[StreamRunnerService, Throwable, Unit] = for
    _            <- zlog("Application starting")
    streamRunner <- ZIO.service[StreamRunnerService]
    _            <- streamRunner.run
  yield ()

  // The source resolves the sink table's JSON pointer property, so it needs a SinkPropertyManager. That service is also
  // published by frameworkPipelineServicesLayer, which in turn requires a StreamingSource for the stream finalizer.
  // Feeding the source its own sink property manager breaks that dependency cycle.
  val pullStreamingSourceLayer: ZLayer[
    PluginStreamContext & DynamoDbClient,
    Throwable,
    PullStreamingSource & SchemaProvider[ArcaneSchema]
  ] =
    IcebergTablePropertyManager.sinkLayer >>> PullStreamingSource.getLayer(context =>
      context.asInstanceOf[PullStreamPluginContext].source.configuration
    )

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
    Services.sourceLayer,
    LayerAssemblies.frameworkPipelineServicesLayer,
    LayerAssemblies.frameworkStagingServicesLayer,
    PullStreamPluginContext.layer,
    dynamoDbClientLayer,
    pullStreamingSourceLayer,
    GenericStreamRunnerService.layer,
    StreamGraphResolver.composedLayer
  )

  @main
  def run: ZIO[Any, Throwable, Unit] = streamRunner.handleAppFailure(exit)

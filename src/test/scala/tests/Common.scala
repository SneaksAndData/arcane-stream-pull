package com.sneaksanddata.arcane.stream_pull
package tests

import main.{appLayer, dynamoDbClientLayer, pullStreamingSourceLayer}

import com.sneaksanddata.arcane.framework.models.app.PluginStreamContext
import com.sneaksanddata.arcane.framework.plugins.LayerAssemblies
import com.sneaksanddata.arcane.framework.plugins.pullstream.Services
import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, StreamGraphResolver}
import com.sneaksanddata.arcane.framework.testkit.appbuilder.TestAppBuilder.buildTestApp
import zio.ZIO
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig
import zio.ZLayer

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
      streamContextLayer: ZLayer[
        Any,
        Nothing,
        PluginStreamContext & DatagramSocketConfig & MetricsConfig & DatadogPublisherConfig
      ]
  ): ZIO[Any, Throwable, Unit] =
    buildTestApp(
      appLayer,
      streamContextLayer
    )(
      dynamoDbClientLayer,
      pullStreamingSourceLayer,
      Services.sourceLayer,
      LayerAssemblies.frameworkPipelineServicesLayer,
      LayerAssemblies.frameworkStagingServicesLayer,
      GenericStreamRunnerService.layer,
      StreamGraphResolver.composedLayer
    )

  val TargetDecoder: ResultSet => (String, String, String, String) =
    (rs: ResultSet) =>
      (
        rs.getString(1),
        rs.getString(2),
        rs.getString(3),
        rs.getString(4)
      )

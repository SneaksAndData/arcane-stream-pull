package com.sneaksanddata.arcane.pull_stream_plugin_context
package models.app

import com.sneaksanddata.arcane.framework.models.app.{
  DefaultOverrideStreamContext,
  DefaultPluginStreamContext,
  OverrideStreamContext,
  PluginStreamContext
}
import com.sneaksanddata.arcane.framework.models.settings.observability.{
  DefaultObservabilitySettings,
  DefaultOverrideObservabilitySettings
}
import com.sneaksanddata.arcane.framework.models.settings.sink.{DefaultOverrideSinkSettings, DefaultSinkSettings}
import com.sneaksanddata.arcane.framework.models.settings.sources.OverrideStreamSourceSettings
import com.sneaksanddata.arcane.framework.models.settings.staging.{
  DefaultOverrideStagingSettings,
  DefaultStagingSettings
}
import com.sneaksanddata.arcane.framework.models.settings.streaming.{
  DefaultOverrideStreamModeSettings,
  DefaultOverrideThroughputSettings,
  DefaultStreamModeSettings,
  DefaultThroughputSettings
}
import upickle.default.*
import upickle.implicits.key
import zio.ZLayer
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig

/** The specification for the stream.
  */
case class PullStreamPluginContext(
    @key("observability") private val observabilityIn: DefaultObservabilitySettings,
    @key("staging") private val stagingIn: DefaultStagingSettings,
    @key("streamMode") private val streamModeIn: DefaultStreamModeSettings,
    @key("sink") private val sinkIn: DefaultSinkSettings,
    @key("throughput") private val throughputIn: DefaultThroughputSettings,
    override val source: PullStreamSourceSettings
) extends DefaultPluginStreamContext(observabilityIn, stagingIn, streamModeIn, sinkIn, throughputIn) derives ReadWriter:
  // TODO: should be implemented when Operator supports overrides
  override def merge[OtherImpl <: OverrideStreamContext](other: Option[OtherImpl]): this.type = this

object PullStreamPluginContext:
  def apply(value: String): PullStreamPluginContext = PluginStreamContext[PullStreamPluginContext](value)

  lazy val layer
      : ZLayer[Any, Throwable, PluginStreamContext & DatagramSocketConfig & MetricsConfig & DatadogPublisherConfig] =
    PluginStreamContext.getLayer[PullStreamPluginContext, PullStreamOverrideStreamContext]

/** Overrides for the stream specification, supplied by the operator via STREAMCONTEXT__SPEC_OVERRIDE.
  */
case class PullStreamOverrideStreamContext(
    @key("observability") override val observability: Option[DefaultOverrideObservabilitySettings] = None,
    @key("staging") override val staging: Option[DefaultOverrideStagingSettings] = None,
    @key("streamMode") override val streamMode: Option[DefaultOverrideStreamModeSettings] = None,
    @key("sink") override val sink: Option[DefaultOverrideSinkSettings] = None,
    @key("throughput") override val throughput: Option[DefaultOverrideThroughputSettings] = None
) extends DefaultOverrideStreamContext(streamMode, sink, staging, observability, throughput) derives ReadWriter:
  // TODO: source overrides should be supported when Operator supports overrides
  override val source: Option[OverrideStreamSourceSettings] = None

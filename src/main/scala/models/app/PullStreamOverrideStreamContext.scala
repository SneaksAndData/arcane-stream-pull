package com.sneaksanddata.arcane.pull_stream_plugin_context
package models.app

import com.sneaksanddata.arcane.framework.models.app.DefaultOverrideStreamContext
import com.sneaksanddata.arcane.framework.models.settings.observability.DefaultOverrideObservabilitySettings
import com.sneaksanddata.arcane.framework.models.settings.sink.DefaultOverrideSinkSettings
import com.sneaksanddata.arcane.framework.models.settings.staging.DefaultOverrideStagingSettings
import com.sneaksanddata.arcane.framework.models.settings.streaming.{
  DefaultOverrideStreamModeSettings,
  DefaultOverrideThroughputSettings
}
import upickle.ReadWriter

/** The partial stream override provided by the Arcane operator via the environment variable.
  */
case class PullStreamOverrideStreamContext(
    override val streamMode: Option[DefaultOverrideStreamModeSettings] = None,
    override val sink: Option[DefaultOverrideSinkSettings] = None,
    override val staging: Option[DefaultOverrideStagingSettings] = None,
    override val observability: Option[DefaultOverrideObservabilitySettings] = None,
    override val throughput: Option[DefaultOverrideThroughputSettings] = None,
    override val source: Option[OverridePullStreamSourceSettings] = None
) extends DefaultOverrideStreamContext(streamMode, sink, staging, observability, throughput) derives ReadWriter

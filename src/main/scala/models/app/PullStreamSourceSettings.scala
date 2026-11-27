package com.sneaksanddata.arcane.pull_stream_plugin_context
package models.app

import com.sneaksanddata.arcane.framework.models.settings.sources.modification.DefaultDataRowModificationSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.{DefaultSourceBufferingSettings, StreamSourceSettings}
import com.sneaksanddata.arcane.framework.models.settings.sources.pullstream.DefaultPullStreamSourceSettings
import upickle.ReadWriter

case class PullStreamSourceSettings(
    override val buffering: DefaultSourceBufferingSettings,
    override val modifications: DefaultDataRowModificationSettings,
    override val configuration: DefaultPullStreamSourceSettings
) extends StreamSourceSettings derives ReadWriter:
  override type SourceSettingsType = DefaultPullStreamSourceSettings

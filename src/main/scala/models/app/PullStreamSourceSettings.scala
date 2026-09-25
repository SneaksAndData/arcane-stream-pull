package com.sneaksanddata.arcane.pull_stream_plugin_context
package models.app

import com.sneaksanddata.arcane.framework.models.settings.DefaultFieldSelectionRuleSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.{DefaultSourceBufferingSettings, StreamSourceSettings}
import com.sneaksanddata.arcane.framework.models.settings.sources.modification.DefaultDataRowModificationSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.pullstream.{
  PullStreamSourceSettings,
  DefaultPullStreamSourceSettings
}
import upickle.ReadWriter

case class PullStreamSourceSettings(
    override val buffering: DefaultSourceBufferingSettings,
    override val fieldSelectionRule: DefaultFieldSelectionRuleSettings,
    override val configuration: DefaultPullStreamSourceSettings,
    override val modifications: DefaultDataRowModificationSettings = DefaultDataRowModificationSettings(Seq.empty)
) extends StreamSourceSettings derives ReadWriter:
  override type SourceSettingsType = DefaultPullStreamSourceSettings

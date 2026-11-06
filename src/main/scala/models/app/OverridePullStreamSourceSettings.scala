package com.sneaksanddata.arcane.pull_stream_plugin_context
package models.app

import com.sneaksanddata.arcane.framework.models.settings.sources.modification.DefaultOverrideDataRowModificationSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.pullstream.DefaultPullStreamSourceSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.{
  DefaultSourceBufferingSettings,
  OverrideStreamSourceSettings
}
import upickle.ReadWriter

/** A partial override of `PullStreamSourceSettings` provided by the Arcane operator via the environment variable. Not
  * applied yet, see the TODO note on `PullStreamPluginContext.merge`.
  */
case class OverridePullStreamSourceSettings(
    override val configuration: Option[DefaultPullStreamSourceSettings] = None,
    override val buffering: Option[DefaultSourceBufferingSettings] = None,
    override val modifications: Option[DefaultOverrideDataRowModificationSettings] = None
) extends OverrideStreamSourceSettings derives ReadWriter:
  override type SourceSettingsOverrideType = DefaultPullStreamSourceSettings

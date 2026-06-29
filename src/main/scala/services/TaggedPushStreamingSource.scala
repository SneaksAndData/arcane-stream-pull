package com.sneaksanddata.arcane.stream_pull
package services

import com.sneaksanddata.arcane.framework.models.app.PluginStreamContext
import com.sneaksanddata.arcane.framework.models.schemas.{
  ArcaneSchema,
  IndexedField,
  IndexedMergeKeyField,
  MergeKeyField
}
import com.sneaksanddata.arcane.framework.models.settings.sources.pushstream.PushStreamSourceSettings
import com.sneaksanddata.arcane.framework.services.base.SchemaProvider
import com.sneaksanddata.arcane.framework.services.iceberg.base.SinkPropertyManager
import com.sneaksanddata.arcane.framework.services.pushstream.PushStreamingSource
import com.sneaksanddata.arcane.framework.services.pushstream.versioning.PushStreamWatermark
import com.sneaksanddata.arcane.framework.services.streaming.base.StructuredZStream
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

/** Wraps [[PushStreamingSource]] to fix the implicit `Iceberg.Schema -> ArcaneSchema` conversion: when an iceberg
  * column named "ARCANE_MERGE_KEY" already exists, the framework returns it as a plain [[IndexedField]] and the
  * resulting schema therefore has no merge key marker, causing
  * `PushStreamChangeTrackingMergeBatch.batchSchema.mergeKey` to fail with "MergeKeyField must be defined for the schema
  * to be usable for merges".
  *
  * This subclass re-tags that column as [[IndexedMergeKeyField]] so downstream merge batch construction works.
  */
final class TaggedPushStreamingSource(
    settings: PushStreamSourceSettings,
    dynamodbClient: DynamoDbClient,
    sinkPropertyManager: SinkPropertyManager
) extends PushStreamingSource(settings, dynamodbClient, sinkPropertyManager):

  private def tagMergeKey(schema: ArcaneSchema): ArcaneSchema =
    val patched = schema.map {
      case f: IndexedField if f.name.equalsIgnoreCase(MergeKeyField.name) =>
        IndexedMergeKeyField(fieldId = f.fieldId)
      case other => other
    }
    ArcaneSchema(patched)

  override def getSchema: Task[ArcaneSchema] = super.getSchema.map(tagMergeKey)

  override def getChanges(previousVersion: PushStreamWatermark): ZStream[Any, Throwable, StructuredZStream] =
    super.getChanges(previousVersion).map { case (rowStream, schema) => (rowStream, tagMergeKey(schema)) }

object TaggedPushStreamingSource:
  private type SettingsExtractor = PluginStreamContext => PushStreamSourceSettings
  private type Environment       = PluginStreamContext & DynamoDbClient & SinkPropertyManager

  def getLayer(
      extractor: SettingsExtractor
  ): ZLayer[Environment, Nothing, PushStreamingSource & SchemaProvider[ArcaneSchema]] =
    ZLayer {
      for
        context         <- ZIO.service[PluginStreamContext]
        client          <- ZIO.service[DynamoDbClient]
        propertyManager <- ZIO.service[SinkPropertyManager]
      yield new TaggedPushStreamingSource(extractor(context), client, propertyManager)
    }

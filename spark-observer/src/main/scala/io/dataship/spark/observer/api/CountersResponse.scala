package io.dataship.spark.observer.api

import java.util.LinkedHashMap

import io.dataship.spark.observer.events.ObserverEvent

final case class CountersResponse(
    schemaVersion: String,
    pluginVersion: String,
    sparkVersion: String,
    appId: String,
    mode: String,
    capturedAt: String,
    receivedByCategory: Map[String, Long],
    listenerReceived: Long,
    processed: Long,
    queued: Long,
    inFlight: Long,
    droppedByPlugin: Long,
    internalFailures: Long,
    depth: Long,
    capacity: Int,
    lastEventAt: String,
    invariantHolds: Boolean
) {
  private[api] def allowlistedValues: java.util.Map[String, Object] = {
    val values = new LinkedHashMap[String, Object]()
    val categories = new LinkedHashMap[String, Object]()
    ObserverEvent.Categories.foreach { category =>
      categories.put(
        category,
        Long.box(receivedByCategory.getOrElse(category, 0L))
      )
    }
    values.put("schemaVersion", schemaVersion)
    values.put("pluginVersion", pluginVersion)
    values.put("sparkVersion", sparkVersion)
    values.put("appId", appId)
    values.put("mode", mode)
    values.put("capturedAt", capturedAt)
    values.put("receivedByCategory", categories)
    values.put("listenerReceived", Long.box(listenerReceived))
    values.put("processed", Long.box(processed))
    values.put("queued", Long.box(queued))
    values.put("inFlight", Long.box(inFlight))
    values.put("droppedByPlugin", Long.box(droppedByPlugin))
    values.put("internalFailures", Long.box(internalFailures))
    values.put("depth", Long.box(depth))
    values.put("capacity", Int.box(capacity))
    values.put("lastEventAt", lastEventAt)
    values.put("invariantHolds", Boolean.box(invariantHolds))
    values
  }
}

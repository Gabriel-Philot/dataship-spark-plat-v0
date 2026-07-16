package io.dataship.spark.observer.api

import java.util.LinkedHashMap

final case class HealthResponse(
    schemaVersion: String,
    pluginVersion: String,
    sparkVersion: String,
    appId: String,
    mode: String,
    capturedAt: String,
    status: String,
    uiAttached: Boolean,
    listenerInstalled: Boolean,
    supportedRuntime: Boolean,
    queueCapacity: Int,
    lastErrorCode: String
) {
  private[api] def allowlistedValues: java.util.Map[String, Object] = {
    val values = new LinkedHashMap[String, Object]()
    values.put("schemaVersion", schemaVersion)
    values.put("pluginVersion", pluginVersion)
    values.put("sparkVersion", sparkVersion)
    values.put("appId", appId)
    values.put("mode", mode)
    values.put("capturedAt", capturedAt)
    values.put("status", status)
    values.put("uiAttached", Boolean.box(uiAttached))
    values.put("listenerInstalled", Boolean.box(listenerInstalled))
    values.put("supportedRuntime", Boolean.box(supportedRuntime))
    values.put("queueCapacity", Int.box(queueCapacity))
    values.put("lastErrorCode", lastErrorCode)
    values
  }
}

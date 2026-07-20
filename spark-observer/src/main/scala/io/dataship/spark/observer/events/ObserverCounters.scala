package io.dataship.spark.observer.events

final case class ObserverCounters(
    listenerReceived: Long,
    processed: Long,
    queued: Long,
    inFlight: Long,
    droppedByPlugin: Long,
    recentEvents: Vector[ObserverEvent]
) {
  val invariantHolds: Boolean =
    listenerReceived == processed + queued + inFlight + droppedByPlugin
}

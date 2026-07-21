package io.dataship.spark.observer.events

final case class ObserverCounters(
    listenerReceived: Long,
    processed: Long,
    queued: Long,
    inFlight: Long,
    droppedByPlugin: Long,
    receivedByCategory: Map[String, Long],
    internalFailures: Long,
    lastEventAt: Option[java.time.Instant],
    recentEvents: Vector[ObserverEvent]
) {
  val invariantHolds: Boolean =
    listenerReceived == processed + queued + inFlight + droppedByPlugin
}

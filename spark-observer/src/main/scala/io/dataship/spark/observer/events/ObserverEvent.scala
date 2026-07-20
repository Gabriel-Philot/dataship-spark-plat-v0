package io.dataship.spark.observer.events

import java.time.Instant

final case class ObserverEvent(category: String, observedAt: Instant)

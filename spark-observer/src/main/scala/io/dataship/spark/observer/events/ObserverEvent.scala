package io.dataship.spark.observer.events

import java.time.Instant

final case class ObserverEvent(category: String, observedAt: Instant)

object ObserverEvent {
  val Categories: Vector[String] = Vector(
    "application",
    "job",
    "stage",
    "task",
    "sql",
    "executor",
    "other"
  )

  def normalizedCategory(category: String): String =
    if (Categories.contains(category)) category else "other"
}

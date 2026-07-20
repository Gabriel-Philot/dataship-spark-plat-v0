package io.dataship.spark.observer.events

import java.util.concurrent.ArrayBlockingQueue

import scala.collection.mutable

final class ObserverState(val transitionsCapacity: Int) {
  require(transitionsCapacity > 0, "transitionsCapacity must be positive")

  private val recentEvents = mutable.Queue.empty[ObserverEvent]
  private var listenerReceived = 0L
  private var processed = 0L
  private var queued = 0L
  private var inFlight = 0L
  private var droppedByPlugin = 0L

  private[events] def offer(
      queue: ArrayBlockingQueue[ObserverEvent],
      event: ObserverEvent
  ): Boolean = synchronized {
    listenerReceived += 1L
    if (queue.offer(event)) {
      queued += 1L
      true
    } else {
      droppedByPlugin += 1L
      false
    }
  }

  private[events] def reject(): Boolean = synchronized {
    listenerReceived += 1L
    droppedByPlugin += 1L
    false
  }

  private[events] def beginProcessing(
      queue: ArrayBlockingQueue[ObserverEvent]
  ): Option[ObserverEvent] = synchronized {
    Option(queue.poll()).map { event =>
      queued -= 1L
      inFlight += 1L
      event
    }
  }

  private[events] def discardQueued(
      queue: ArrayBlockingQueue[ObserverEvent]
  ): Unit = synchronized {
    var discarded = queue.poll()
    while (discarded != null) {
      queued -= 1L
      droppedByPlugin += 1L
      discarded = queue.poll()
    }
  }

  private[events] def completeProcessing(
      event: ObserverEvent
  ): Unit = synchronized {
    inFlight -= 1L
    processed += 1L
    if (recentEvents.size == transitionsCapacity) {
      recentEvents.dequeue()
    }
    recentEvents.enqueue(event)
  }

  def snapshot: ObserverCounters = synchronized {
    ObserverCounters(
      listenerReceived = listenerReceived,
      processed = processed,
      queued = queued,
      inFlight = inFlight,
      droppedByPlugin = droppedByPlugin,
      recentEvents = recentEvents.toVector
    )
  }
}

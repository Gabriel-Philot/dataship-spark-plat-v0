package io.dataship.spark.observer.events

import java.time.{Clock, Instant, ZoneOffset}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.dataship.v412.Spark412Bridge
import org.scalatest.funsuite.AnyFunSuite

final class ObserverListenerSpec extends AnyFunSuite {
  private val fixedInstant = Instant.parse("2026-07-21T10:15:30Z")
  private val fixedClock = Clock.fixed(fixedInstant, ZoneOffset.UTC)

  test("uses the dedicated DataShip listener-bus queue name") {
    assert(Spark412Bridge.ListenerQueueName == "dataship-observer")
  }

  test("classifies every fixed category and hands each event to the bounded queue") {
    val sqlEvent = new SparkListenerEvent {}
    val otherEvent = new SparkListenerEvent {}
    val state = new ObserverState(transitionsCapacity = 8)
    val queue = new BoundedEventQueue(capacity = 16, state = state)
    val listener = new ObserverListener(
      queue = queue,
      clock = fixedClock,
      isSqlEvent = event => event eq sqlEvent
    )

    try {
      listener.onApplicationStart(null)
      listener.onJobStart(null)
      listener.onStageSubmitted(null)
      listener.onTaskStart(null)
      listener.onExecutorAdded(null)
      listener.onOtherEvent(sqlEvent)
      listener.onOtherEvent(otherEvent)

      awaitCondition("all classified events to finish processing") {
        state.snapshot.processed == 7L
      }

      val listenerSnapshot = listener.snapshot
      val queueSnapshot = state.snapshot
      assert(listenerSnapshot.receivedByCategory == Map(
        "application" -> 1L,
        "job" -> 1L,
        "stage" -> 1L,
        "task" -> 1L,
        "sql" -> 1L,
        "executor" -> 1L,
        "other" -> 1L
      ))
      assert(listenerSnapshot.internalFailures == 0L)
      assert(listenerSnapshot.lastEventAt.contains(fixedInstant))
      assert(queueSnapshot.listenerReceived == 7L)
      assert(queueSnapshot.invariantHolds)
    } finally {
      queue.close()
    }
  }

  test("returns promptly when the plugin queue refuses a listener event") {
    val processingStarted = new CountDownLatch(1)
    val releaseProcessing = new CountDownLatch(1)
    val state = new ObserverState(transitionsCapacity = 4)
    val queue = new BoundedEventQueue(
      capacity = 1,
      state = state,
      process = _ => {
        processingStarted.countDown()
        releaseProcessing.await(5, TimeUnit.SECONDS)
      }
    )
    val listener = new ObserverListener(queue = queue, clock = fixedClock)

    try {
      listener.onJobStart(null)
      assert(processingStarted.await(2, TimeUnit.SECONDS))
      listener.onJobStart(null)

      val refusalStartedAt = System.nanoTime()
      listener.onJobStart(null)
      val refusalMillis =
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - refusalStartedAt)

      val snapshot = state.snapshot
      assert(refusalMillis < 250L)
      assert(listener.snapshot.receivedByCategory("job") == 3L)
      assert(snapshot.listenerReceived == 3L)
      assert(snapshot.queued == 1L)
      assert(snapshot.inFlight == 1L)
      assert(snapshot.droppedByPlugin == 1L)
      assert(snapshot.invariantHolds)
    } finally {
      releaseProcessing.countDown()
      queue.close()
    }
  }

  private def awaitCondition(description: String)(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
    while (!condition && System.nanoTime() < deadline) {
      Thread.sleep(5L)
    }
    assert(condition, s"Timed out waiting for $description")
  }
}

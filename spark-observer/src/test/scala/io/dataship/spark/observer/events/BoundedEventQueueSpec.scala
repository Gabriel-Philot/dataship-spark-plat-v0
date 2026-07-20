package io.dataship.spark.observer.events

import java.time.Instant
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.scalatest.funsuite.AnyFunSuite

final class BoundedEventQueueSpec extends AnyFunSuite {
  test("accounts a null event as a refused offer without breaking the invariant") {
    val state = new ObserverState(transitionsCapacity = 2)
    val queue = new BoundedEventQueue(capacity = 1, state = state)

    try {
      assert(!queue.offer(null))

      val snapshot = state.snapshot
      assert(snapshot.listenerReceived == 1L)
      assert(snapshot.processed == 0L)
      assert(snapshot.queued == 0L)
      assert(snapshot.inFlight == 0L)
      assert(snapshot.droppedByPlugin == 1L)
      assert(snapshot.invariantHolds)
    } finally {
      queue.close()
    }
  }

  test("reclassifies queued events as dropped when shutdown stops the worker") {
    val processingStarted = new CountDownLatch(1)
    val releaseProcessing = new CountDownLatch(1)
    val state = new ObserverState(transitionsCapacity = 2)
    val queue = new BoundedEventQueue(
      capacity = 1,
      state = state,
      process = _ => {
        processingStarted.countDown()
        releaseProcessing.await(5, TimeUnit.SECONDS)
      }
    )

    assert(queue.offer(event(1)))
    assert(processingStarted.await(2, TimeUnit.SECONDS))
    assert(queue.offer(event(2)))

    queue.close()
    queue.close()
    releaseProcessing.countDown()

    val snapshot = state.snapshot
    assert(!queue.isWorkerAlive)
    assert(snapshot.listenerReceived == 2L)
    assert(snapshot.processed == 1L)
    assert(snapshot.queued == 0L)
    assert(snapshot.inFlight == 0L)
    assert(snapshot.droppedByPlugin == 1L)
    assert(snapshot.invariantHolds)
  }

  test("refuses a third event promptly while one event is in flight and one is queued") {
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

    try {
      assert(queue.offer(event(1)))
      assert(processingStarted.await(2, TimeUnit.SECONDS))
      assert(queue.offer(event(2)))

      val refusalStartedAt = System.nanoTime()
      assert(!queue.offer(event(3)))
      val refusalMillis =
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - refusalStartedAt)
      val fullSnapshot = state.snapshot

      assert(refusalMillis < 250L)
      assert(fullSnapshot.listenerReceived == 3L)
      assert(fullSnapshot.processed == 0L)
      assert(fullSnapshot.queued == 1L)
      assert(fullSnapshot.inFlight == 1L)
      assert(fullSnapshot.droppedByPlugin == 1L)
      assert(fullSnapshot.invariantHolds)
      info(
        s"full queue: received=${fullSnapshot.listenerReceived}, " +
          s"queued=${fullSnapshot.queued}, inFlight=${fullSnapshot.inFlight}, " +
          s"processed=${fullSnapshot.processed}, " +
          s"dropped=${fullSnapshot.droppedByPlugin}, " +
          s"offerMillis=$refusalMillis"
      )

      releaseProcessing.countDown()
      awaitCondition("both accepted events to be processed") {
        state.snapshot.processed == 2L
      }

      val completedSnapshot = state.snapshot
      assert(completedSnapshot.listenerReceived == 3L)
      assert(completedSnapshot.processed == 2L)
      assert(completedSnapshot.queued == 0L)
      assert(completedSnapshot.inFlight == 0L)
      assert(completedSnapshot.droppedByPlugin == 1L)
      assert(completedSnapshot.invariantHolds)
      assert(completedSnapshot.recentEvents == Vector(event(1), event(2)))
    } finally {
      releaseProcessing.countDown()
      queue.close()
      queue.close()
    }

    assert(!queue.isWorkerAlive)
  }

  private def event(sequence: Long): ObserverEvent =
    ObserverEvent(
      category = "other",
      observedAt = Instant.ofEpochSecond(sequence)
    )

  private def awaitCondition(description: String)(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
    while (!condition && System.nanoTime() < deadline) {
      Thread.sleep(5L)
    }
    assert(condition, s"Timed out waiting for $description")
  }
}

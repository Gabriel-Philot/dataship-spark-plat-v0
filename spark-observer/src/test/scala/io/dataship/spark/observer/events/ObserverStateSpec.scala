package io.dataship.spark.observer.events

import java.time.Instant
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import io.dataship.spark.observer.{ObserverConfig, ObserverRuntime}
import org.apache.spark.SparkConf
import org.scalatest.funsuite.AnyFunSuite

final class ObserverStateSpec extends AnyFunSuite {
  test("keeps every concurrent snapshot consistent") {
    val producerCount = 6
    val offersPerProducer = 200
    val expectedReceived = producerCount.toLong * offersPerProducer
    val start = new CountDownLatch(1)
    val producersFinished = new CountDownLatch(producerCount)
    val sampling = new AtomicBoolean(true)
    val observedFailure = new AtomicReference[Throwable]()
    val state = new ObserverState(transitionsCapacity = 8)
    val queue = new BoundedEventQueue(capacity = 16, state = state)
    val executor = Executors.newFixedThreadPool(producerCount + 1)

    try {
      executor.submit(new Runnable {
        override def run(): Unit = {
          try {
            while (sampling.get()) {
              val current = state.snapshot
              if (!current.invariantHolds) {
                throw new AssertionError(s"Inconsistent snapshot: $current")
              }
              if (current.recentEvents.size > 8) {
                throw new AssertionError(s"Recent window exceeded capacity: $current")
              }
            }
          } catch {
            case error: Throwable => observedFailure.compareAndSet(null, error)
          }
        }
      })

      (0 until producerCount).foreach { producer =>
        executor.submit(new Runnable {
          override def run(): Unit = {
            try {
              start.await()
              (0 until offersPerProducer).foreach { index =>
                queue.offer(event(producer.toLong * offersPerProducer + index))
              }
            } finally {
              producersFinished.countDown()
            }
          }
        })
      }

      start.countDown()
      assert(producersFinished.await(10, TimeUnit.SECONDS))
      awaitCondition("all received events to be processed or dropped") {
        val current = state.snapshot
        current.listenerReceived == expectedReceived &&
        current.queued == 0L &&
        current.inFlight == 0L
      }
    } finally {
      sampling.set(false)
      queue.close()
      queue.close()
      executor.shutdownNow()
      assert(executor.awaitTermination(5, TimeUnit.SECONDS))
    }

    Option(observedFailure.get()).foreach(throw _)
    val completed = state.snapshot
    assert(completed.listenerReceived == expectedReceived)
    assert(completed.invariantHolds)
    assert(completed.recentEvents.size <= 8)
    assert(!queue.isWorkerAlive)
    info(
      s"concurrent final: received=${completed.listenerReceived}, " +
        s"processed=${completed.processed}, dropped=${completed.droppedByPlugin}, " +
        s"window=${completed.recentEvents.size}/8"
    )
  }

  test("evicts completed transitions in FIFO order") {
    val state = new ObserverState(transitionsCapacity = 3)
    val queue = new BoundedEventQueue(capacity = 4, state = state)
    val events = (1L to 5L).map(event).toVector

    try {
      events.zipWithIndex.foreach { case (nextEvent, index) =>
        assert(queue.offer(nextEvent))
        awaitCondition(s"event ${index + 1} to be processed") {
          state.snapshot.processed == index + 1L
        }
      }

      val snapshot = state.snapshot
      assert(snapshot.recentEvents == events.takeRight(3))
      assert(snapshot.recentEvents.size == 3)
      assert(snapshot.invariantHolds)
    } finally {
      queue.close()
    }
  }

  test("parses the fixed-capacity transition window contract") {
    val defaultConfig = ObserverConfig.from(new SparkConf(false))
    val minimum = ObserverConfig.from(
      new SparkConf(false)
        .set("spark.dataship.observer.transitions.capacity", "1")
    )
    val maximum = ObserverConfig.from(
      new SparkConf(false)
        .set("spark.dataship.observer.transitions.capacity", "1024")
    )

    assert(defaultConfig.transitionsCapacity == 128)
    assert(minimum.transitionsCapacity == 1)
    assert(maximum.transitionsCapacity == 1024)

    Seq("0", "1025", "not-an-integer").foreach { value =>
      val error = intercept[IllegalArgumentException] {
        ObserverConfig.from(
          new SparkConf(false)
            .set("spark.dataship.observer.transitions.capacity", value)
        )
      }
      assert(
        error.getMessage.contains(
          "spark.dataship.observer.transitions.capacity"
        )
      )
    }
  }

  test("lets an enabled runtime own and repeatedly close its worker") {
    val runtime = new ObserverRuntime(
      config = ObserverConfig(
        enabled = true,
        queueCapacity = 2,
        transitionsCapacity = 2
      ),
      sparkVersion = "4.1.2"
    )
    val queue = runtime.eventQueue.getOrElse(
      fail("An enabled runtime must own an event queue")
    )

    assert(queue.isWorkerAlive)
    runtime.shutdown()
    runtime.shutdown()

    assert(!queue.isWorkerAlive)
  }

  private def event(sequence: Long): ObserverEvent =
    ObserverEvent(
      category = "other",
      observedAt = Instant.ofEpochSecond(sequence)
    )

  private def awaitCondition(description: String)(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
    while (!condition && System.nanoTime() < deadline) {
      Thread.sleep(5L)
    }
    assert(condition, s"Timed out waiting for $description")
  }
}

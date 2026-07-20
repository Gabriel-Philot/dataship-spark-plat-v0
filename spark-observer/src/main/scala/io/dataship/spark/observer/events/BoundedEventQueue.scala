package io.dataship.spark.observer.events

import java.util.concurrent.{ArrayBlockingQueue, Semaphore, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

final class BoundedEventQueue(
    val capacity: Int,
    val state: ObserverState,
    process: ObserverEvent => Unit = _ => ()
) extends AutoCloseable {
  require(capacity > 0, "capacity must be positive")

  private val CloseTimeoutMillis = TimeUnit.SECONDS.toMillis(5L)
  private val events = new ArrayBlockingQueue[ObserverEvent](capacity)
  private val available = new Semaphore(0)
  private val closed = new AtomicBoolean(false)
  private val lifecycleLock = new Object
  private val worker = new Thread(
    () => runWorker(),
    "dataship-observer-worker"
  )
  worker.setDaemon(true)
  worker.start()

  def offer(event: ObserverEvent): Boolean = lifecycleLock.synchronized {
    if (event == null || closed.get()) {
      state.reject()
    } else {
      val accepted = state.offer(events, event)
      if (accepted) {
        available.release()
      }
      accepted
    }
  }

  def isWorkerAlive: Boolean = worker.isAlive

  override def close(): Unit = {
    val shouldInterrupt = lifecycleLock.synchronized {
      closed.compareAndSet(false, true)
    }
    if (shouldInterrupt) {
      worker.interrupt()
    }
    try {
      if (Thread.currentThread() ne worker) {
        worker.join(CloseTimeoutMillis)
      }
    } finally {
      state.discardQueued(events)
    }
  }

  private def runWorker(): Unit = {
    var running = true
    while (running) {
      try {
        available.acquire()
        if (closed.get()) {
          running = false
        } else {
          state.beginProcessing(events).foreach { event =>
            var interrupted = false
            try {
              process(event)
            } catch {
              case _: InterruptedException => interrupted = true
            } finally {
              state.completeProcessing(event)
            }
            if (interrupted) {
              running = false
            }
          }
        }
      } catch {
        case _: InterruptedException => running = false
      }
    }
  }
}

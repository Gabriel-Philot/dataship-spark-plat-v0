package org.apache.spark.dataship.v412

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.util

import io.dataship.spark.observer.api.SnapshotStoreNotReadyException
import org.apache.spark.status.{AppStatusStore, JobDataWrapper, StageDataWrapper}
import org.apache.spark.util.kvstore.{KVStore, KVStoreIterator, KVStoreView}
import org.scalatest.funsuite.AnyFunSuite

final class Spark412SnapshotSourceSpec extends AnyFunSuite {
  test("bounds native job and stage views to the requested limit plus one") {
    val jobs = new RecordingView[JobDataWrapper](Vector.empty)
    val stages = new RecordingView[StageDataWrapper](Vector.empty)
    val source = new Spark412SnapshotSource(
      new AppStatusStore(recordingStore(jobs, stages), None, None)
    )

    assert(source.jobs(5).isEmpty)
    assert(source.stages(2).isEmpty)

    assert(jobs.reverseCalled)
    assert(jobs.maximum == 6L)
    assert(jobs.iteratorClosed)
    assert(stages.reverseCalled)
    assert(stages.maximum == 3L)
    assert(stages.iteratorClosed)
  }

  test("closes the native iterator when DTO mapping fails") {
    val jobs = new RecordingView[JobDataWrapper](Vector(null))
    val stages = new RecordingView[StageDataWrapper](Vector.empty)
    val source = new Spark412SnapshotSource(
      new AppStatusStore(recordingStore(jobs, stages), None, None)
    )

    intercept[NullPointerException](source.jobs(1))

    assert(jobs.iteratorClosed)
  }

  test("translates only missing startup application information to store not ready") {
    val jobs = new RecordingView[JobDataWrapper](Vector.empty)
    val stages = new RecordingView[StageDataWrapper](Vector.empty)
    val source = new Spark412SnapshotSource(
      new AppStatusStore(recordingStore(jobs, stages), None, None)
    )

    intercept[SnapshotStoreNotReadyException](source.application())
  }

  private def recordingStore(
      jobs: RecordingView[JobDataWrapper],
      stages: RecordingView[StageDataWrapper]
  ): KVStore = {
    Proxy
      .newProxyInstance(
        getClass.getClassLoader,
        Array(classOf[KVStore]),
        new InvocationHandler {
          override def invoke(
              proxy: Object,
              method: Method,
              arguments: Array[Object]
          ): Object = method.getName match {
            case "view" =>
              arguments(0) match {
                case clazz if clazz == classOf[JobDataWrapper] => jobs
                case clazz if clazz == classOf[StageDataWrapper] => stages
                case _ => new RecordingView[AnyRef](Vector.empty)
              }
            case "close" => null
            case "toString" => "recording-kv-store"
            case _ => throw new UnsupportedOperationException(method.getName)
          }
        }
      )
      .asInstanceOf[KVStore]
  }

  private final class RecordingView[T](values: Vector[T])
      extends KVStoreView[T] {
    var reverseCalled = false
    var maximum = -1L
    var iteratorClosed = false

    override def reverse(): KVStoreView[T] = {
      reverseCalled = true
      super.reverse()
    }

    override def max(value: Long): KVStoreView[T] = {
      maximum = value
      super.max(value)
    }

    override def iterator(): util.Iterator[T] = closeableIterator()

    override def closeableIterator(): KVStoreIterator[T] =
      new KVStoreIterator[T] {
        private var offset = 0

        override def hasNext: Boolean = offset < values.size

        override def next(): T = {
          if (!hasNext) {
            throw new util.NoSuchElementException("recording iterator is empty")
          }
          val value = values(offset)
          offset += 1
          value
        }

        override def next(maximum: Int): util.List[T] = {
          val result = new util.ArrayList[T]()
          var remaining = maximum
          while (remaining > 0 && hasNext) {
            result.add(next())
            remaining -= 1
          }
          result
        }

        override def skip(count: Long): Boolean = {
          offset = math.min(values.size, offset + count.toInt)
          hasNext
        }

        override def close(): Unit = iteratorClosed = true
      }
  }
}

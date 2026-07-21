package io.dataship.spark.observer.api

trait SparkSnapshotSource {
  def application(): ApplicationView

  /**
   * Returns the newest records first and may include one extra record as a
   * truncation sentinel. Implementations must never fetch more than
   * `limit + 1` records.
   */
  def jobs(limit: Int): Seq[JobView]

  /** Same bounded sentinel contract as [[jobs]]. */
  def stages(limit: Int): Seq[StageView]

  def sqlExecutions(limit: Int): Seq[SqlExecutionView]
}

final class SnapshotStoreNotReadyException(message: String)
    extends RuntimeException(message)

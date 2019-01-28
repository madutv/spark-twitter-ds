package org.abyas.twitter

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader

/**
  * Implementation of InputPartitionReader for Twitter Stream. This class
  * takes in a ListBuffer of Any Seq and the elements of Seq will be
  * spread as columns in InternalRow
  * @param block
  */
class TwitterInputPartitionReader(block: ListBuffer[Seq[Any]]) extends InputPartitionReader[InternalRow] {

  private var currentIdx: Int = -1

  /**
    * Increments index for retrieval
    * @return
    */
  override def next(): Boolean = {
    currentIdx += 1
    currentIdx < block.size
  }

  /**
    * Get current index
    * @return
    */
  override def get(): InternalRow = {
    val tweet = block(currentIdx)
    println(s"tweet: $tweet")
    InternalRow(tweet: _*)
  }

  override def close(): Unit = {}

}

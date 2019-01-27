package org.abyas.twitter

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader

class TwitterInputPartitionReader(block: ListBuffer[Seq[Any]]) extends InputPartitionReader[InternalRow] {

  private var currentIdx: Int = -1

  override def next(): Boolean = {
    currentIdx += 1
    currentIdx < block.size
  }

  override def get(): InternalRow = {
    val tweet = block(currentIdx)
    println(s"tweet: $tweet")
    InternalRow(tweet: _*)
  }

  override def close(): Unit = {}

}

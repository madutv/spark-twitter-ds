package org.abyas.twitter

import java.util.Optional

import scala.collection.JavaConverters._
import org.abyas.UnitTester
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.types.DoubleType

import scala.collection.mutable.ListBuffer

class TestTwitterMicroBatchReader extends UnitTester{

  val map: Map[String, String] = Map()
  val twitterOptions: TwitterOptions = new TwitterOptions(new DataSourceOptions(map.asJava))
  val twitterSchema: TwitterSchema = new TwitterSchema()
  val twitterMicroBatchReader: TwitterMicroBatchReader = new TwitterMicroBatchReader(twitterOptions, twitterSchema)
  val listAfterCommit = ListBuffer(Seq(8905, "XYZ", Array("x", "y", "z")),
                                   Seq(890, "XYZ", Array("x", "y", "z")))

  twitterMicroBatchReader.tweetList = ListBuffer(
                                      Seq(123, "ABC", Array("a", "b", "c")),
                                      Seq(8910, "XYZ", Array("x", "y", "z")),
                                      Seq(8905, "XYZ", Array("x", "y", "z")),
                                      Seq(890, "XYZ", Array("x", "y", "z")))


  test("setOffsetRange, getStartOffset, getEndOffset : Set Start and End Offset") {
    twitterMicroBatchReader.setOffsetRange(Optional.of(LongOffset(0L)), Optional.of(LongOffset(2L)))
    assert(twitterMicroBatchReader.getStartOffset.json.toLong == 0L)
    assert(twitterMicroBatchReader.getEndOffset.json.toLong == 2L)
  }

  test("deserializeOffset: should convert json string to LongOffset"){
    assert(twitterMicroBatchReader.deserializeOffset("5") == LongOffset(5L))
  }

  test("readSchema: should return Schema from TwitterSchema"){
    assert(twitterMicroBatchReader.readSchema().equals(twitterSchema.Schema))
  }

  test("Commit should trim tweetList "){
    twitterMicroBatchReader.commit(LongOffset(1L))
    twitterMicroBatchReader.tweetList.zip(listAfterCommit).foreach(a => {
      if(!a._1.isInstanceOf[AnyRef])
        assert(a._1 == a._2)
      else assert(a._1.toArray.deep == a._2.toArray.deep)
    })
  }

  test("planInputPartitions: Not sure yet"){
    val items = twitterMicroBatchReader.planInputPartitions()
    val inputPart = items.get(0).createPartitionReader()
    inputPart.next()
    assert(inputPart.get.get(0, DoubleType) == 8905.0)
  }

}

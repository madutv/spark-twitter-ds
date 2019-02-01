package org.abyas.twitter


import java.util.concurrent.TimeUnit
import java.util.{Optional, Properties}
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.types.DoubleType

import org.abyas.UnitTester
import org.abyas.twitter.TwitterConsts._


class TestTwitterMicroBatchReader extends UnitTester{

  val prop: Properties = new Properties()
  prop.load(scala.io.Source.fromFile("./src/test/resources/secret.properties").reader())

  val ck = prop.getProperty(CONSUMER_KEY)
  val cs = prop.getProperty(CONSUMER_SECRET)
  val ak = prop.getProperty(ACCESS_TOKEN)
  val ask = prop.getProperty(ACCESS_TOKEN_SECRET)

  val cks = Map(CONSUMER_KEY -> ck, CONSUMER_SECRET -> cs, ACCESS_TOKEN -> ak, ACCESS_TOKEN_SECRET -> ask)
  val dso = new DataSourceOptions(cks.asJava)

  val twitterOptions: TwitterOptions = new TwitterOptions(dso)

  val twitterMicroBatchReader: TwitterMicroBatchReader = new TwitterMicroBatchReader(twitterOptions)
  //TimeUnit.SECONDS.sleep(10)
  twitterMicroBatchReader.stop()

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
   assert(twitterMicroBatchReader.readSchema().equals(TwitterSchema.Schema))
 }

 test("Commit should trim tweetList "){
   twitterMicroBatchReader.commit(LongOffset(1L))
   twitterMicroBatchReader.tweetList.zip(listAfterCommit).foreach(a => {
     if(!a._1.isInstanceOf[AnyRef])
       assert(a._1 == a._2)
     else assert(a._1.toArray.deep == a._2.toArray.deep)
   })
 }

 test("planInputPartitions: reader is created and able to extract from list"){
   val items = twitterMicroBatchReader.planInputPartitions()
   val inputPart = items.get(0).createPartitionReader()
   inputPart.next()
   assert(inputPart.get.get(0, DoubleType) == 8905.0)
 }

  test("initialize & receive & TwitterStatusListener together will had " +
    "tweets to queue and tweet list "){
    twitterMicroBatchReader.tweetQueue = null
    twitterMicroBatchReader.tweetList = ListBuffer()
    twitterMicroBatchReader.start()
    TimeUnit.SECONDS.sleep(10)
    twitterMicroBatchReader.tweetQueue.poll(10000, TimeUnit.MILLISECONDS)
    //println(s"First element from TweetList ${twitterMicroBatchReader.tweetList.head}")
    assert(!twitterMicroBatchReader.tweetList.isEmpty)
  }

}

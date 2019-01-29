package org.abyas.twitter

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, TimeUnit}
import java.util.Optional

import com.fasterxml.jackson.databind.JsonNode
import javax.annotation.concurrent.GuardedBy
import org.abyas.utils.json.{JsonHelpers, JsonTypesToSparkTypes}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.json4s.JValue
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods.{asJsonNode, compact, parse}
import twitter4j._

/**
  * Implements MicroBatchReader for Twitter
  * @param twitterOptions: Holds twitter options specified
  *                      as part of spark.read. Options available are
  *                      CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET
  *                      follow, track, locations, languages, columns
  *                      NUM_PARTITIONS, QUEUE_SIZE
  * @param schema Twitter Schema object that hold schema details passed from
  *               spark.read
  */
//class TwitterMicroBatchReader(twitterOptions: TwitterOptions,
//                              schema: TwitterSchema) extends MicroBatchReader {

class TwitterMicroBatchReader(twitterOptions: TwitterOptions) extends  MicroBatchReader{

  private val numPartitions: Int = twitterOptions.numPartitions
  private val queueSize: Int = twitterOptions.queueSize


  private var startOffset: Offset = _
  private var endOffset: Offset = _

  @GuardedBy("this")
  private var currentOffset: LongOffset = LongOffset(-1)

  @GuardedBy("this")
  private var lastOffsetCommitted: LongOffset = LongOffset(-1)

  private var stopped = false
  private var incomingEventCounter = 0

  @GuardedBy("this")
  private var worker: Thread = _

  private var twitterStream: TwitterStream = _
  @GuardedBy("this")
  var tweetList: ListBuffer[Seq[Any]] = ListBuffer[Seq[Any]]()
  var tweetQueue: BlockingQueue[Seq[Any]] = _

  initialize()

  def initialize(): Unit = synchronized {
    tweetQueue = new ArrayBlockingQueue[Seq[Any]](queueSize)
    twitterStream = twitterOptions.createTwitterStream()
    twitterStream.addListener(new TwitterStatusListener(tweetQueue))

    worker = new Thread("Tweet Worker") {
      setDaemon(true)
      override def run(): Unit = {
        receive()
      }
    }
    worker.start()
    twitterOptions.filterQuery match{
      case None => twitterStream.sample("en")
      case Some(a) => twitterStream.filter(a)
    }
  }

  private def receive(): Unit = {
    while(!stopped) {
      val tweet: Seq[Any] = tweetQueue.poll(100, TimeUnit.MILLISECONDS)
      if(tweet != null) {
        tweetList.append(tweet)
        currentOffset = currentOffset + 1
        incomingEventCounter = incomingEventCounter + 1
      }
    }
  }


  /**
    * Set Start and End offset.
    * @param start
    * @param end
    */
  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    startOffset = start.orElse(LongOffset(-1L))
    endOffset = end.orElse(currentOffset)
  }

  /**
    * Return Start offset
    * @return
    */
  override def getStartOffset: Offset = {
    Option(startOffset).getOrElse(throw new IllegalStateException("start offset not set"))
  }

  /**
    * Returns end offset
    * @return
    */
  override def getEndOffset: Offset = {
    Option(endOffset).getOrElse(throw new IllegalStateException("end offset not set"))
  }

  override def deserializeOffset(json: String): Offset = LongOffset(json.toLong)

  override def readSchema(): StructType = {
    //schema
    //schema.Schema
    TwitterSchema.Schema
  }

  override def commit(end: Offset): Unit = synchronized {
    val newOffset: LongOffset = LongOffset.convert(end).getOrElse(sys.error(s"Unknonw offset type $end"))
    val diffOffSet = (newOffset.offset - lastOffsetCommitted.offset).asInstanceOf[Int]
    if (diffOffSet < 0) sys.error(s"Out of order offest $newOffset $lastOffsetCommitted")

    tweetList.trimStart(diffOffSet)
    lastOffsetCommitted = newOffset
  }

  override def stop(): Unit = {
    stopped = true
    if (twitterStream != null)
      try
        twitterStream.shutdown()
      catch {
        case e: Exception => println(e)
      }
  }

  override def planInputPartitions(): java.util.List[InputPartition[InternalRow]] = {
    val startOrdinal = LongOffset.convert(startOffset).get.offset.toInt + 1
    val endOrdinal = LongOffset.convert(endOffset).get.offset.toInt + 1

    val newBlocks = synchronized {
      val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
      val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
      assert(sliceStart <= sliceEnd, s"sliceStart: $sliceStart sliceEnd: $sliceEnd")
      tweetList.slice(sliceStart, sliceEnd)
    }

    //val spark = SparkSession.getActiveSession.get
    //val numPartitions = spark.sparkContext.defaultParallelism

    newBlocks.grouped(numPartitions).map { block =>
      new InputPartition[InternalRow] {
        override def createPartitionReader(): InputPartitionReader[InternalRow] =
          new TwitterInputPartitionReader(block)
      }
    }.toList.asJava
  }
}

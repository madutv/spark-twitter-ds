package org.abyas.twitter

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, TimeUnit}
import java.util.Optional
import javax.annotation.concurrent.GuardedBy
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}

import twitter4j.TwitterStream

/**
  * Implements MicroBatchReader for Twitter
  * @param twitterOptions: Holds twitter options specified
  *                      as part of spark.read. Options available are
  *                      CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET
  *                      follow, track, locations, languages, columns
  *                      NUM_PARTITIONS, QUEUE_SIZE, TWITTER_POLL_TIMEOUT
  */
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

  /**
    * This method listens to tweets, processes them to required
    * type based on "columns" and schema provided during spark
    * read and adds it into a Queue. If filters are provided,
    * only tweets matching the filters are listened to otherwise,
    * sample tweets as provided by twitter will be processed
    */
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

  /**
    * Reads from Queue and appends to a List. This is list that
    * will be read be read from InputPartitionReader
    */
  private def receive(): Unit = {
    while(!stopped) {
      val tweet: Seq[Any] = tweetQueue.poll(twitterOptions.qpoll, TimeUnit.MILLISECONDS)
      if(tweet != null) {
        tweetList.append(tweet)
        currentOffset = currentOffset + 1
        incomingEventCounter = incomingEventCounter + 1
      }
    }
  }


  /**
    * Set Start and End offset.
    * @param start Start Offset
    * @param end: End Offset
    */
  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    startOffset = start.orElse(LongOffset(-1L))
    endOffset = end.orElse(currentOffset)
  }

  /**
    * Return Start offset
    * @return Start offset
    */
  override def getStartOffset: Offset = {
    Option(startOffset).getOrElse(throw new IllegalStateException("start offset not set"))
  }

  /**
    * Returns end offset
    * @return End Offset
    */
  override def getEndOffset: Offset = {
    Option(endOffset).getOrElse(throw new IllegalStateException("end offset not set"))
  }

  /**
    * Converts json string to LongOffset
    * @param json: json string of number
    * @return
    */
  override def deserializeOffset(json: String): Offset = LongOffset(json.toLong)

  /**
    * Returns schema as specified in spark.read or returns a
    * default Schema which is simply a StructType with a StringType
    * StructField
    * @return schema StructType
    */
  override def readSchema(): StructType = TwitterSchema.Schema

  /**
    * Tracks offsets that are read. List will be trimmed upto
    * commited offset
    * @param end Offset upto which to commit
    */
  override def commit(end: Offset): Unit = synchronized {
    val newOffset: LongOffset = LongOffset.convert(end).getOrElse(sys.error(s"Unknonw offset type $end"))
    val diffOffSet = (newOffset.offset - lastOffsetCommitted.offset).asInstanceOf[Int]
    if (diffOffSet < 0) sys.error(s"Out of order offest $newOffset $lastOffsetCommitted")

    tweetList.trimStart(diffOffSet)
    lastOffsetCommitted = newOffset
  }

  /**
    * If twitterStream is null, tries to shutdown the twitter stream
    */
  override def stop(): Unit = {
    stopped = true
    if (twitterStream != null)
      try
        twitterStream.shutdown()
      catch {
        case e: Exception => println(e)
      }
  }

  /**
    * Helper method to restart streaming
    */
  def start(): Unit = {
    stopped = false
    initialize()
  }

  /**
    * Determines items that need to be read from List. This then creates
    * TwitterInputParitionReader with the data from list.
    * @return : List of TwitterInputPartitionReader
    */
  override def planInputPartitions(): java.util.List[InputPartition[InternalRow]] = {
    val startOrdinal = LongOffset.convert(startOffset).get.offset.toInt + 1
    val endOrdinal = LongOffset.convert(endOffset).get.offset.toInt + 1

    val newBlocks = synchronized {
      val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
      val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
      assert(sliceStart <= sliceEnd, s"sliceStart: $sliceStart sliceEnd: $sliceEnd")
      tweetList.slice(sliceStart, sliceEnd)
    }
    newBlocks.grouped(numPartitions).map { block =>
      new InputPartition[InternalRow] {
        override def createPartitionReader(): InputPartitionReader[InternalRow] =
          new TwitterInputPartitionReader(block)
      }
    }.toList.asJava
  }
}

package org.abyas.twitter

import java.util.concurrent.BlockingQueue

import org.apache.spark.unsafe.types.UTF8String

import org.json4s.JValue
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods.{asJsonNode, compact, parse}
import com.fasterxml.jackson.databind.JsonNode
import twitter4j.{StatusListener, Status, TwitterObjectFactory, StatusDeletionNotice, StallWarning}

import org.abyas.utils.json.{JsonHelpers, JsonTypesToSparkTypes}

/**
  * Implements Twitter's StatusListener
  * @param tweetQueue Twitter Queue
  */
class TwitterStatusListener(tweetQueue: BlockingQueue[Seq[Any]]) extends StatusListener {
  /**
    * Once the Status is received, this method determins columns requested and
    * Schema type defined and converts status to required types and adds it to
    * a queue, which is then picked up by another method for further processing
    * @param status
    */
  override def onStatus(status: Status): Unit = {
    val jval: JValue = parse(TwitterObjectFactory.getRawJSON(status))
    val colsAndSchema = (TwitterSchema.cols, TwitterSchema.schemaColumns)
    val tweetJson: Seq[Any] =
      colsAndSchema match {
         case(a, b) if a.isEmpty && b.head.equals("twitter") => Seq(UTF8String.fromString(compact(jval)))
         case(_, b) =>
           val aJval: Seq[JValue] = TwitterSchema.cols.map(a => JsonHelpers.extractNestedJvalsExact(jval, a)).toSeq
           if(b.length == 1 & b.head.equals("twitter"))
             Seq(UTF8String.fromString(compact(JArray(aJval.toList))))
           else{
             val aJNode: Seq[JsonNode] = aJval.map(j => asJsonNode(j))
             JsonTypesToSparkTypes.matchJsonNodesToSparkTypes(TwitterSchema.Schema, aJNode)
           }
      }
    tweetQueue.add(tweetJson)
  }
  override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}
  override def onTrackLimitationNotice(i: Int): Unit = {}
  override def onScrubGeo(l: Long, l1: Long): Unit = {}
  override def onStallWarning(stallWarning: StallWarning): Unit = {}
  override def onException(e: Exception): Unit = {}

}

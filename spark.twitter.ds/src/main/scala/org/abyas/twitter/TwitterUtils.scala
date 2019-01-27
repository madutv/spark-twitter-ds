package org.abyas.twitter

import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.json4s.JValue
import org.abyas.twitter.TwitterConsts._
import org.abyas.utils.implicits.JavaToScalaImplicits.optionalToOption
import org.abyas.utils.implicits.OptionImplicits.OptionAToAImplicit
import twitter4j.{TwitterStream, TwitterStreamFactory}
import twitter4j.conf.ConfigurationBuilder

object TwitterUtils {

  def extractTwitterColumn(jval: JValue, cols: Array[String]): JValue = {
    if(cols.isEmpty) return jval
    extractTwitterColumn(jval.\(cols.head), cols.tail)
  }

  def createTwitterFilters(options: DataSourceOptions): TwitterFilters = {
    val follow: Option[String] = options.get(FOLLOW)
    val track: Option[String] = options.get(TRACK)
    val locations: Option[String] = options.get(LOCATIONS)
    val languages: Option[String] = options.get(LANGUAGES)
    val columns: Option[String] = options.get(TWITTER_COLUMNS)

    new TwitterFilters(
      ofollow = follow,
      otrack = track,
      olocations = locations,
      olanguages = languages,
      ocolumns = columns)
  }

  def createTwitterStream(options: DataSourceOptions): TwitterStream = {
    val ck: Option[String] = options.get(CONSUMER_KEY)
    val cs: Option[String] = options.get(CONSUMER_SECRET)
    val at: Option[String] = options.get(ACCESS_TOKEN)
    val ats: Option[String] = options.get(ACCESS_TOKEN_SECRET)

    val configBuilder: ConfigurationBuilder = new ConfigurationBuilder()
    configBuilder.setJSONStoreEnabled(true)
    configBuilder.setOAuthConsumerKey(ck.getOrFail("Failed to find Consumer Key"))
      .setOAuthConsumerSecret(cs.getOrFail("Failed to find Consumer Secret"))
      .setOAuthAccessToken(at.getOrFail("Failed to find Access Token"))
      .setOAuthAccessTokenSecret(ats.getOrFail("Failed to find Access Token Secret"))

    new TwitterStreamFactory(configBuilder.build()).getInstance()
  }



}

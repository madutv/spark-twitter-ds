package org.abyas.twitter

import org.apache.spark.sql.sources.v2.DataSourceOptions

import twitter4j.conf.ConfigurationBuilder
import twitter4j.{FilterQuery, TwitterStream, TwitterStreamFactory}

import org.abyas.twitter.TwitterConsts._
import org.abyas.utils.implicits.JavaToScalaImplicits.optionalToOption
import org.abyas.utils.implicits.OptionImplicits.OptionAToAImplicit
import org.abyas.utils.implicits.StringImplicits.StringImplicit
import org.abyas.utils.implicits.OptionImplicits.OptionStringImplicit

/**
  * Takes in DataSources Options and dose the following
  * 1) Sets Twitter Secrets. createTwitterStream can be used to
  * create Twitter Stream at later time
  * 2) Create Twitter Filters if any of follow, tract, locations
  * or languages are set.
  * 3) If columns are specified, creates an array of columns to be
  * extracted
  *
  * @param options DataSourceOptions like:
  *                twitter secret options:
  *                 CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET
  *                twitter filter options:
  *                 follow, track, locations, languages
  *                twitter columns to extract: columns
  *                 NUM_PARTITIONS, QUEUE_SIZE and TWITTER_POLL_TIMEOUT
  */
class TwitterOptions(options: DataSourceOptions) {

  //Twitter Secrets
  private val ck: Option[String] = options.get(CONSUMER_KEY)
  private val cs: Option[String] = options.get(CONSUMER_SECRET)
  private val at: Option[String] = options.get(ACCESS_TOKEN)
  private val ats: Option[String] = options.get(ACCESS_TOKEN_SECRET)

  //Twitter Filters
  private val ofollow: Option[String] = options.get(FOLLOW)
  private val otrack: Option[String] = options.get(TRACK)
  private val olocations: Option[String] = options.get(LOCATIONS)
  private val olanguages: Option[String] = options.get(LANGUAGES)

  //Create Twitter Filter Parameters
  val follow: Array[Long] = ofollow.toLongArray(PRIMARY_DELIMITER)
  val track: Array[String] = otrack.toStringArray(PRIMARY_DELIMITER)
  val locations: Array[Array[Double]] = olocations
                                          .toStringArray(PRIMARY_DELIMITER)
                                          .map(a => a.toDoubleArray(SECONDARY_DELIMITER))
  val languages: Array[String] = olanguages.toStringArray(PRIMARY_DELIMITER)

  //Twitter Columns
  private val ocolumns: Option[String] = options.get(TWITTER_COLUMNS)
  private val columns: Array[String] = ocolumns.toStringArray(PRIMARY_DELIMITER)

  //Twitter Queue and Partitions
  val numPartitions: Int = options.get(NUM_PARTITIONS).orElse("5").toInt
  val queueSize: Int = options.get(QUEUE_SIZE).orElse("512").toInt
  val qpoll: Int = options.get(TWITTER_POLL_TIMEOUT).orElse("5000").toInt

  /*
  Create Twitter Stream, Twitter Filter and Twitter Filter Columns
   */
  //val twitterStream: TwitterStream = createTwitterStream()
  val filterQuery: Option[FilterQuery] = createTwitterFilter()
  val filterColumns: Array[Array[String]] = createTwitterFilterColumns()

  /**
    * Creates TwitterStream based on Consumer key, secret, token and
    * token secret specified in spark read options
    * @return TwitterStream if it was created successfully
    */
  def createTwitterStream(): TwitterStream = {
    val configBuilder: ConfigurationBuilder = new ConfigurationBuilder()
    configBuilder.setJSONStoreEnabled(true)
    configBuilder.setOAuthConsumerKey(ck.getOrFail("Failed to find Consumer Key"))
      .setOAuthConsumerSecret(cs.getOrFail("Failed to find Consumer Secret"))
      .setOAuthAccessToken(at.getOrFail("Failed to find Access Token"))
      .setOAuthAccessTokenSecret(ats.getOrFail("Failed to find Access Token Secret"))

    new TwitterStreamFactory(configBuilder.build()).getInstance()
  }

  /**
    * This method checks if any of follow, track, locations, lanaguages are
    * provided, if so, then a Twitter's FilterQuery will be created based
    * on them. Else None will be returned
    *
    * @return FilterQuery or None
    */
  def createTwitterFilter(): Option[FilterQuery] = {
    if (follow.isEmpty && track.isEmpty && locations.isEmpty && languages.isEmpty)
      None
    else
      Some(new FilterQuery(0, follow, track, locations, languages))
  }

  /**
    * This method converts columns from Map[String, String] to
    * Map[String, Array[String]], where the Array[String] is the depth from where
    * to extract Twitter contents. For example, to get expanded_urls we'd have
    * Array["entities", "urls", "expanded_url"]
    *
    * @return
    */
  def createTwitterFilterColumns(): Array[Array[String]] = {
    columns match {
      case a if a.isEmpty => Array()
      case a => a.map(v => v.toStringArray(PATH_DELIMITER))
    }
  }



}

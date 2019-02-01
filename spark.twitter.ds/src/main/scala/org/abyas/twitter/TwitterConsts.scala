package org.abyas.twitter

/**
  * Object to store application constants
  */
object TwitterConsts {

  lazy val CONSUMER_KEY = "CONSUMER_KEY"
  lazy val CONSUMER_SECRET = "CONSUMER_SECRET"
  lazy val ACCESS_TOKEN = "ACCESS_TOKEN"
  lazy val ACCESS_TOKEN_SECRET = "ACCESS_TOKEN_SECRET"
  lazy val SECRET_PROPERTY_FILE = "SECRET_PROPERTY_FILE"

  lazy val FOLLOW = "follow"
  lazy val TRACK = "track"
  lazy val LOCATIONS = "locations"
  lazy val LANGUAGES = "languages"
  lazy val TWITTER_COLUMNS = "columns"

  lazy val NUM_PARTITIONS = "NUM_PARTITIONS"
  lazy val QUEUE_SIZE = "QUEUE_SIZE"
  lazy val TWITTER_POLL_TIMEOUT = "TWITTER_POLL_TIMEOUT"

  lazy val PRIMARY_DELIMITER = ","
  lazy val SECONDARY_DELIMITER = ";"
  lazy val PATH_DELIMITER = "->"

}
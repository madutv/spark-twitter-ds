package org.abyas.twitter


import java.util.Optional

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport}
import org.apache.spark.internal.Logging


/**
  * Implemetation of DataSourceV2 for twitter.
  */
class DefaultSource extends  DataSourceV2 with MicroBatchReadSupport
with DataSourceRegister with Logging {

  /**
    * Short Name
    * @return: Short name for Datasource
    */
  override def shortName(): String = "twitter"

  /**
    * Implementation of createMicroBatchReader. This method takes in
    * StructType if provided, and options that were passed in during
    * spark read and processes them. Options available for options are:
    * twitter secret options:
    * CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET
    * twitter filter options:
    * follow, track, locations, languages
    * twitter columns to extract: columns
    * NUM_PARTITIONS, QUEUE_SIZE and TWITTER_POLL_TIMEOUT
    *
    *
    * @param schema Optional[StructType]. If provided, this will be used
    *               as StructType, otherwise a default StructType of StringType
    *               will be used, in which case entire tweet will be considered
    *               a column of String
    * @param checkpointLocation: Checkpoint Location
    * @param options: Options in spark.read as described above
    * @return TwitterMicroBatchReader
    */
  override def createMicroBatchReader(schema: Optional[StructType],
                                      checkpointLocation: String,
                                      options: DataSourceOptions): MicroBatchReader = {

    val twitterOptions: TwitterOptions = new TwitterOptions(options)
    TwitterSchema.setSchema(schema)

    TwitterSchema.setRequestedColumns(twitterOptions.filterColumns)

    new TwitterMicroBatchReader(twitterOptions)

  }

}

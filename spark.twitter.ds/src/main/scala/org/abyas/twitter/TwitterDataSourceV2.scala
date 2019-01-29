package org.abyas.twitter


import java.util.Optional

import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.abyas.utils.implicits.JavaToScalaImplicits.optionalToOption


class DefaultSource extends  DataSourceV2 with MicroBatchReadSupport
with DataSourceRegister with Logging {

  override def shortName(): String = "twitter"

  override def createMicroBatchReader(schema: Optional[StructType],
                                      checkpointLocation: String,
                                      options: DataSourceOptions): MicroBatchReader = {

    val twitterOptions: TwitterOptions = new TwitterOptions(options)
    TwitterSchema.setSchema(schema)
    TwitterSchema.setRequestedColumns(twitterOptions.filterColumns)
    //val twitterSchema: TwitterSchema = TwitterSchema(schema)
    //twitterSchema.setSchema(schema)
    //twitterSchema.setRequestedColumns(twitterOptions.filterColumns)

   // new TwitterMicroBatchReader(twitterOptions, twitterSchema)
    val schemaRefined =
      if(schema.isEmpty) StructType(StructField("twitter", StringType) :: Nil) else schema.get()

   // new TwitterMicroBatchReader(twitterOptions, twitterSchema)
    new TwitterMicroBatchReader(twitterOptions)
  }

}

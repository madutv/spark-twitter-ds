package org.abyas

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._

/**
 * @author ${user.name}
 */
object App {
  
  def main(args : Array[String]) {
    println( "World Peace" )
   // Logger.getLogger("org").setLevel(Level.ERROR)
   // Logger.getLogger("akka").setLevel(Level.ERROR)


    val twitOps: Map[String, String] = Map(
      "CONSUMER_KEY" -> "",
      "CONSUMER_SECRET" -> "",
      "ACCESS_TOKEN" -> "-",
      "ACCESS_TOKEN_SECRET" -> "",
      //"follow" -> "759251, 54956563, 199819724, 2167904845")
      "columns" -> "id, user -> name, text, entities -> urls ")

    val struct: StructType = StructType(
      StructField("id", LongType)
        :: StructField("name", StringType)
        :: StructField("text", StringType)
        //:: StructField("urls", ArrayType(StringType, true))
        :: StructField("urls", ArrayType(StructType(Array(
        StructField("url", StringType),
        StructField("expanded_url", StringType),
        StructField("display_url", StringType),
        StructField("indices", ArrayType(LongType))
      )), true))
        //:: StructField("urls", ArrayType(
        //  DataTypes.createMapType(StringType, StringType)))
        :: Nil
    )




    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._


    val item: Dataset[_] = spark.readStream
      .format("org.abyas.twitter")
      .options(twitOps)
      .schema(struct)
      //.schema(struct2)
      .load()//.sho

    item.writeStream
      .foreachBatch((a, b) => a.show)
      //.foreachBatch((a, b) => a.selectExpr("urls").columns.foreach(c => println(s"******* urls: $c")))
      //.foreachBatch((a, b) => a.selectExpr("urls").collect().foreach(c => println(s"****** $c ***")))
      .start()
      .awaitTermination()

  }

}


## Twitter Streaming DataSource with Spark V2 DataSource

### Quick Usage
```scala
	spark.read
		 .format("org.abyas.twitter")
		 .options(Map[String, String]) //See Options section
		 .schema(StructType) //optional. See Usage section
		 .load()
```

### Options

#### _Twitter Secrets_
`CONSUMER_KEY:` Twitter Consumer Key   
`CONSUMER_SECRET:` Twitter Consumer Secret  
`ACCESS_TOKEN:` Twitter Access Token  
`ACCESS_TOKEN_SECRET:` Twitter Access Token Secret  

#### _Twitter Filters_
These options can be used to filter in specific tweets. This is achieved by constructing Twitter's FilterQuery. Any, all or none of these options may be provided. If none is provided, then sample tweets as provided by TwitterStream streamed. 

`follow:` Comma seperated user ids  
`track:` Comma seperated list of tags/text  
`languages:` Comma seperated list of languages  
`locations:` Comma and -> ";" list of locations. 

```
  "follow" -> "759251, 54956563, 199819724, 2167904845"
  "track" -> "dogs, cats, elephants"
  "languages" -> "en, es"
  "locations" -> "-122.75;36.8, -121.75;37.8"
``` 

#### _Twitter Columns_
There are 2 ways to extract specific columns from tweets. 

*	Through "columns" option
* 	Through "schema"

This section will address the former. See Schema Section for the later.

```
	"columns" -> "id, user -> name, text, entities -> urls -> url"
```
"columns" option will extract data from extact path of json tweets. In the above example:

*	 "id" will be extracted from root level
*	 "name" will be extracted from *user -> name*
*	 "text" will be extracted from root level
*	 "url" will be extracted from *entities -> urls -> url*

Difference between "columns" specified as an option vs through schema is that: "columns" will extract from exact path whereas the later will find the first match.

### Schema
Schema may or may not be specified with spark read. Following are some scenarios:

If Schema is not Specified:

*	If "columns" is not specified as an option: Entier tweet is extracted as a StringType
* 	If "columns" is specified: Columns are extracted from a tweet as a StringType

If Schema is Specified:

*	If "columns" is not specified as an option: "name" in StructField will be used to extract the first occurrence of "name" in a tweet.
*	If "columns" is specified: "columns" will be extracted

Notes:

*	If JsonType is ARRAY, the StructField must be ArrayType 
*  If JsonType is OBJECT, the StructField must be either MapType or StructType

### Usage

#### _Example 1: Sample Tweets_
```scala
val twitOps: Map[String, String] = Map(
      "CONSUMER_KEY" -> "xxxxx",
      "CONSUMER_SECRET" -> "xxxxx",
      "ACCESS_TOKEN" -> "xxxxx",
      "ACCESS_TOKEN_SECRET" -> "xxxxx")
      
val items: Dataset[_] = spark.readStream
      .format("org.abyas.twitter")
      .options(twitOps)
      .load()
      
items.writeStream
     .foreachBatch((a, b) => a.show)
     .start()
     .awaitTermination()
     
********Output*********
+--------------------+
|             twitter|
+--------------------+
|{"created_at":"Sa...|
|{"created_at":"Sa...|
|{"created_at":"Sa...|
|{"created_at":"Sa...|
+--------------------+

```

#### _Example 2: Specific Users and Columns, No Schema_
```scala
val twitOps: Map[String, String] = Map(
      "CONSUMER_KEY" -> "xxxxx",
      "CONSUMER_SECRET" -> "xxxxx",
      "ACCESS_TOKEN" -> "xxxxx",
      "ACCESS_TOKEN_SECRET" -> "xxxxx",
      "follow" -> "759251, 54956563, 199819724, 2167904845",
      "columns" -> "id, user -> name, text, entities -> urls -> url")
      
val items: Dataset[_] = spark.readStream
      .format("org.abyas.twitter")
      .options(twitOps)
      .load()
      
items.writeStream
     .foreachBatch((a, b) => a.show)
     .start()
     .awaitTermination()
     
********Output*********
+--------------------+
|             twitter|
+--------------------+
|[1094223899720597...|
|[1094223901062660...|
+--------------------+

```

#### _Example 3: Specific Tracks and Languages, With Schema_

```scala

val twitOps: Map[String, String] = Map(
      "CONSUMER_KEY" -> "xxxxx",
      "CONSUMER_SECRET" -> "xxxxx",
      "ACCESS_TOKEN" -> "xxxxx",
      "ACCESS_TOKEN_SECRET" -> "xxxxx",
      "track" -> "crowdfunding, photography, cryptocurrency, pets, funny, movies, bitcoin, java, scala, python",
      "languages" -> "en")
      
val struct: StructType = StructType(
           StructField("id", LongType)
        :: StructField("name", StringType)
        :: StructField("text", StringType)
        :: StructField("url", StringType)
        :: Nil
    )      
      
val items: Dataset[_] = spark.readStream
      .format("org.abyas.twitter")
      .options(twitOps)
      .schema(struct)
      .load()
      
items.writeStream
     .foreachBatch((a, b) => a.show)
     .start()
     .awaitTermination()


********Output*********
+-------------------+-------------------+--------------------+--------------------+
|                 id|               name|                text|                 url|
+-------------------+-------------------+--------------------+--------------------+
|1094226917828501505|              ia :(|one of the best m...|                null|
|1094226918252179456| Cards Are Humorous|BC: Autopsy resul...|https://www.cards...|
|1094226919036411905|               èden|RT @FootballVines...|                null|
|1094226919875379201|  Ocado Tech España|Ocado Technology ...|http://www.ocadot...|
|1094226918398926848|       marcusdennis|Long/Short Bitcoi...|                null|
|1094226920114397184|Aline with an E /🔑|RT @IetarianaIive...|                null|
+-------------------+-------------------+--------------------+--------------------+

```

#### _Example 4: Specific Tracks and Languages, With Nested Schema_

```scala

val twitOps: Map[String, String] = Map(
      "CONSUMER_KEY" -> "xxxxx",
      "CONSUMER_SECRET" -> "xxxxx",
      "ACCESS_TOKEN" -> "xxxxx",
      "ACCESS_TOKEN_SECRET" -> "xxxxx",
      "track" -> "crowdfunding, photography, cryptocurrency, pets, funny, movies, bitcoin, java, scala, python",
      "languages" -> "en")

val struct: StructType = StructType(
      StructField("id", LongType)
        :: StructField("name", StringType)
        :: StructField("text", StringType)
        :: StructField("urls", ArrayType(StructType(Array(
		        StructField("url", StringType),
		        StructField("expanded_url", StringType),
		        StructField("display_url", StringType),
		        StructField("indices", ArrayType(LongType))
		        ))
		 	))
        :: Nil
    )
    
val items: Dataset[_] = spark.readStream
      .format("org.abyas.twitter")
      .options(twitOps)
      .schema(struct)
      .load()
      
items.writeStream
     .foreachBatch((a, b) => a.show)
     .start()
     .awaitTermination()
     
     
+-------------------+---------------+--------------------+--------------------+
|                 id|           name|                text|                urls|
+-------------------+---------------+--------------------+--------------------+
|1094227828521033729|         Miss K|RT @SymplySola: F...|                  []|
|1094227828487397376|  Nurse T-shirt|@ANANursingWorld ...|[[https://t.co/z7...|
|1094227828978020352|Fighter of Love|The BeLL Token is...|                  []|
+-------------------+---------------+--------------------+--------------------+

```



#### _Example 5: Specific Tracks, Languages and Columns, With Schema_
```scala
val twitOps: Map[String, String] = Map(
      "CONSUMER_KEY" -> "xxxxx",
      "CONSUMER_SECRET" -> "xxxxx",
      "ACCESS_TOKEN" -> "xxxxx",
      "ACCESS_TOKEN_SECRET" -> "xxxxx",
      "track" -> "crowdfunding, photography, cryptocurrency, pets, funny, movies, bitcoin, java, scala, python",
      "languages" -> "en",
      "columns" -> "id, user -> name, text, entities -> urls -> url")
      
val struct: StructType = StructType(
           StructField("id", LongType)
        :: StructField("name", StringType)
        :: StructField("text", StringType)
        :: StructField("url", StringType)
        :: Nil
    )      
      
val items: Dataset[_] = spark.readStream
      .format("org.abyas.twitter")
      .options(twitOps)
      .schema(struct)
      .load()
      
items.writeStream
     .foreachBatch((a, b) => a.show)
     .start()
     .awaitTermination()
     
     
********Output*********
+-------------------+------------------+--------------------+--------------------+
|                 id|              name|                text|                 url|
+-------------------+------------------+--------------------+--------------------+
|1094225773106352128|       Maoela Jane|RT @arlenybravo1:...|https://t.co/Yy4x...|
|1094225774037557248|    Adithya Rajesh|Next in line Zora...|https://t.co/rldy...|
|1094225775795023872|       ⚜️Codeman⚜️|RT @311: On-Sale ...|https://t.co/CtOP...|
|1094225777627942912|Taming Fred Savage|RT @AintGotNoRest...|https://t.co/isiq...|

```





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
There are 2 ways to extract specific columns. 

TODO

### Usage
TODO


### Credits
*	https://github.com/hienluu/wikiedit-streaming
*  Apache Spark Socket Streaming



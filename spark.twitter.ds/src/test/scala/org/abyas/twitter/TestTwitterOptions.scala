package org.abyas.twitter

import org.abyas.UnitTester
import scala.collection.JavaConverters._
import org.abyas.twitter.TwitterConsts._
import org.apache.spark.sql.sources.v2.DataSourceOptions

class TestTwitterOptions extends UnitTester {

  val optionMaps1: Map[String, String] = Map(CONSUMER_KEY -> "xxxxx",
    LOCATIONS -> "20;30, 30;50",
    FOLLOW -> "123, 343, 2223232323232323",
    LANGUAGES -> "En, zd",
    TWITTER_COLUMNS -> "a -> b -> c, z")

  val optionMaps2: Map[String, String] = Map(TWITTER_COLUMNS -> "a -> b -> c, z")

  val option1 = new TwitterOptions(new DataSourceOptions(optionMaps1.asJava))
  val option2 = new TwitterOptions(new DataSourceOptions(optionMaps2.asJava))

  test("Check if filter1 has language set to Array(En, zd)"){
    assert(option1.languages.deep == Array("En", "zd").deep)
  }

  test("Check if location is set to Array[Array(20, 30), Array(30, 50))"){
    assert(option1.locations.deep == Array(Array(20.0, 30.0), Array(30, 50)).deep)
  }

  test("Check if follow is set to Array(123, 343, 2223232323232323)"){
    assert(option1.follow.deep == Array(123L, 343L,2223232323232323L).deep)
  }

  test("filterColumns is Array(Array(a, b, c), Array(z)"){
    val cols = Array(Array("a", "b", "c"), Array("z"))
    assert(option1.filterColumns.deep == cols.deep)
  }

  test("track is empty Array"){
    assert(option1.track.isEmpty)
  }

  test("Check if Twitter Filter Query is set"){
    assert(option1.filterQuery.isDefined)
  }

  test("Check if Twitter Filter Query is set Empty"){
    assert(option2.filterQuery.isEmpty)
  }

  test("createTwitterStream Throws Error when keys are not present"){
    intercept[Exception]{
      option2.createTwitterStream()
    }
  }

}

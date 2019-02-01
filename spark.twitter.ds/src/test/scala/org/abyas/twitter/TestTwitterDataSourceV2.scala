package org.abyas.twitter


import java.util.{Optional, Properties}

import scala.collection.JavaConverters._
import org.abyas.UnitTester
import org.abyas.twitter.TwitterConsts.{ACCESS_TOKEN, ACCESS_TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET}
import org.apache.spark.sql.sources.v2.DataSourceOptions

class TestTwitterDataSourceV2 extends UnitTester {

  val prop: Properties = new Properties()
  prop.load(scala.io.Source.fromFile("./src/test/resources/secret.properties").reader())

  val ck = prop.getProperty(CONSUMER_KEY)
  val cs = prop.getProperty(CONSUMER_SECRET)
  val ak = prop.getProperty(ACCESS_TOKEN)
  val ask = prop.getProperty(ACCESS_TOKEN_SECRET)

  val cks = Map(CONSUMER_KEY -> ck, CONSUMER_SECRET -> cs, ACCESS_TOKEN -> ak, ACCESS_TOKEN_SECRET -> ask)
  val dso = new DataSourceOptions(cks.asJava)

  val ds = new org.abyas.twitter.DefaultSource()

  test("Check is shortname is twitter"){
    assert(ds.shortName.equals("twitter"))
  }

  test("Check if creation of MicroBatchReader throws exception"){
      val tds = ds.createMicroBatchReader(Optional.empty(), ".", dso)
      assert(tds.isInstanceOf[TwitterMicroBatchReader])
  }


}

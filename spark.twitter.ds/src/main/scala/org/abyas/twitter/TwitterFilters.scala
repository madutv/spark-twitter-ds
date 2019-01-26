package org.abyas.twitter

import com.typesafe.scalalogging.Logger

class TwitterFilters(var ofollow: Option[String] = None,
                     var otrack: Option[String] = None,
                     var olocations: Option[String] = None,
                     var olanguages: Option[String] = None,
                     var ocolumns: Option[String] = None) {

  val logger = Logger("TwitterFilters")

}

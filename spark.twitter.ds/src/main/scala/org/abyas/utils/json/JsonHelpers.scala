package org.abyas.utils.json

import org.json4s.JValue

object JsonHelpers {

  /**
    * Extract JValue based on path specified in cols
    * @param jval Json JValue
    * @param cols path array
    * @return Jvalue after extracting
    */
  def extractNestedJvalsExact(jval: JValue, cols: Array[String]): JValue = {
    if(cols.isEmpty) return jval
    extractNestedJvalsExact(jval.\(cols.head), cols.tail)
  }

  /**
    * Extract JValue based on path specified in cols. This extracts
    * all matches
    * @param jval Json JValue
    * @param cols path array
    * @return Jvalue after extracting
    */
  def extractNestedJvalsAll(jval: JValue, cols: Array[String]): JValue = {
    if(cols.isEmpty) return jval
    extractNestedJvalsAll(jval.\\(cols.head), cols.tail)
  }

}

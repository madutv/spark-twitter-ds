package org.abyas.utils.json

import org.json4s.JValue
import org.json4s.JsonAST.JField

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

  /**
    * This method find the first match in json
    * @param jval Json JValue
    * @param cols path array
    * @return Jvalue after extracting
    */
  def extractNestedFirst(jval: JValue, cols: Array[String]): JValue = {
    if(jval == null) return null
    if(cols.isEmpty) return jval

    val item = cols.head
    val finding = jval.findField{
      case JField(`item`, _) => true
      case _ => false
    }
    finding match {
      case None => return null
      case Some(a) => extractNestedFirst(a._2, cols.tail)
    }
  }

}

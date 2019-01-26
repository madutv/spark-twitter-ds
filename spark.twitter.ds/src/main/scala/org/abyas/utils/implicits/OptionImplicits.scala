package org.abyas.utils.implicits

import com.typesafe.scalalogging.Logger

/**
  * Implicits for Options
  */
object OptionImplicits {

  val logger = Logger("OptionHelpers")

  import org.abyas.utils.implicits.StringImplicits.StringImplicit

  implicit class OptionStringImplicit(opt: Option[String]) {
    /**
      * Converts delimited Option String to Array[String]. If
      * Option is None, then an empty Array will be returned
      * @param delimiter: Delimiter
      * @return Array[String]
      */
    def toStringArray(delimiter: String): Array[String] = {
      opt match {
        case None => Array()
        case Some(a) => a.toStringArray(delimiter)
      }
    }

    /**
      * Converts delimited Option String to Array[Long]. If
      * Option is None, then an empty Array will be returned.
      * If the conversion fails and Exception will be thrown
      * @param delimiter: Delimiter
      * @return Array[Long]
      */
    def toLongArray(delimiter: String): Array[Long] = {
      opt match {
        case None => Array()
        case Some(a) => a.toLongArray(delimiter)
      }
    }

    /**
      * Converts delimited Option String to Array[Double]. If
      * Option is None, then an empty Array will be returned.
      * If the conversion fails and Exception will be thrown
      * @param delimiter: Delimiter
      * @return Array[Double]
      */
    def toDoubleArray(delimiter: String): Array[Double] = {
      opt match {
        case None => Array()
        case Some(a) => a.toDoubleArray(delimiter)
      }
    }
  }

  /**
    * Implict method to convert Option[A]  to A. If
    * Option is None then an exception will be thrown
    *
    * @param opt : Option to convert to A
    * @tparam A
    */
  implicit class OptionAToAImplicit[A](opt: Option[A]) {
    def getOrFail(msgOnFail: String = s"Failed while Converting ${opt} to value"): A = {
      opt match {
        case None => logger.error(msgOnFail + ": " + opt)
          throw new Exception(msgOnFail + ": " + opt)
        case Some(a) => a
      }
    }
  }

}

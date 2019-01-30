package org.abyas.utils.implicits

import com.typesafe.scalalogging.Logger

import scala.util.Try

/**
  * Implicit conversions for String
  */
object StringImplicits {

  val logger = Logger("StringImplicit")

  implicit class StringImplicit(str: String) {
    /**
      * Converts a delimited String to an Array of String. Any Spaces
      * around the delimiter will be trimmed
      *
      * @param delimiter Delimiter to split on
      * @return Array Of String
      */
    def toStringArray(delimiter: String): Array[String] = {
      str.split(delimiter).map(_.trim)
    }

    /**
      * Converts a delimited string of Longs to Array Long. If the conversion
      * fails, an Exception will be thrown
      * @param delimiter Delimiter to split on
      * @return Array of long
      */
    def toLongArray(delimiter: String): Array[Long] = {
      Try(str.split(delimiter).map(_.trim.toLong)) match {
        case scala.util.Failure(exception) => logger.error("Failed to convert " + str + " to Long Array")
          throw new Exception(exception)
        case scala.util.Success(value) => value
      }
    }

    /**
      * Converts a delimited string of doubles to Array of Double. If the
      * conversion fails, an Exception will be thrown
      * @param delimiter: Delimiter to split on
      * @return Array of Double
      */
    def toDoubleArray(delimiter: String): Array[Double] = {
      Try(str.split(delimiter).map(_.trim.toDouble)) match {
        case scala.util.Failure(exception) => logger.error("Failed to convert " + str + " to Double Array")
          throw new Exception(exception)
        case scala.util.Success(value) => value
      }
    }
  }
}

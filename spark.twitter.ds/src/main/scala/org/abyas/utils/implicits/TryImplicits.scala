package org.abyas.utils.implicits

import scala.util.Try
import com.typesafe.scalalogging.Logger



/**
  * Implict for Try
  */
object TryImplicits {

  val logger = Logger("TryImplicits")

  /**
    * Implict class for Try objects
    * @param tryItem Item warpped in Try
    * @tparam A: Any
    */
  implicit class TryAndDefaultImplicityHelpers[A](tryItem: Try[A]) {
    /**
      * This implicit class can be used to unwrap Try[A] to A and if default values
      * should be returned on Failure
      *
      * @param defaultValue default value to be used for Failure
      * @tparam A anything wrapped in Try
      * @return value or default value without
      */
    def tryAndReturnDefault[A](defaultValue: A, msg: Option[String] = None): A = {
      tryItem match {
        case scala.util.Failure(exception) => logger.warn("Got Failure when trying to unwarp " +
              tryItem + " using default value " + defaultValue)
          msg match {
            case None =>
            case Some(message) => logger.warn(message)
          }
          defaultValue
        case scala.util.Success(value: A) => value
      }
    }
  }

}

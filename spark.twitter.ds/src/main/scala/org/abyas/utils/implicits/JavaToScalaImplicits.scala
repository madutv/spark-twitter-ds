package org.abyas.utils.implicits

import java.util.Optional
import com.typesafe.scalalogging.Logger

/**
  * Implicits to convert Java Objects to Scala Objects
  */
object JavaToScalaImplicits {
  val logger = Logger("JavaToScalaImplicits")

  /**
    * Converts Java Optional of type A to Scala Option of type A
    * @param javaOpt: Java Optional to be converted to Option
    * @tparam A Type of Optional
    * @return: Return Option[A]
    */
  implicit def optionalToOption[A](javaOpt: Optional[A]): Option[A] = {
    if (javaOpt.isPresent) Some(javaOpt.get) else None
  }

}

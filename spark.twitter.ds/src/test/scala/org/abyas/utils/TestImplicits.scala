package org.abyas.utils

import java.util.Optional

import scala.util.{Failure, Success, Try}
import org.abyas.UnitTester

import scala.util.parsing.combinator.Parsers

class TestImplicits extends UnitTester {

  test("Implicit Optional[String] should convert to Option[String]"){
    import org.abyas.utils.implicits.JavaToScalaImplicits.optionalToOption
    val javaOpt: Optional[String] = Optional.of("Peace")
    val emptyOptional: Optional[Int] = Optional.empty()
    val scalaOpt: Option[String] = javaOpt
    val scalaEmptyOpt: Option[Int] = emptyOptional
    assert(scalaOpt.get.equals("Peace"))
    assert(scalaEmptyOpt == None)
  }

  {
    import org.abyas.utils.implicits.StringImplicits.StringImplicit
    test("toArrayString should convert 'World, Peace' to Array[World, Peace]") {
      val str = "World, Peace"
      assert(str.toStringArray(",").deep == Array("World", "Peace").deep)
    }

    test("toLongArray should convert '1, 2' to Array[1, 2]") {
      val str = "1, 2"
      assert(str.toLongArray(",").deep == Array(1, 2).deep)
    }

    test("toDoubleArray should convert '1.0, 2.0' to Array[1, 2]") {
      val str = "1.23, 2.34"
      val doub: Array[Double] = Array(1.23, 2.34)
      assert(str.toDoubleArray(",").deep == doub.deep)
    }
  }

  {
    import org.abyas.utils.implicits.OptionImplicits._
    test("toArrayString should convert Some('World, Peace') to Array[World, Peace]") {
      val str: Option[String] = Some("World, Peace")
      assert(str.toStringArray(",").deep == Array("World", "Peace").deep)
    }

    test("toLongArray should convert Some('1, 2') to Array[1, 2]") {
      val str = Some("1, 2")
      assert(str.toLongArray(",").deep == Array(1, 2).deep)
    }

    test("toDoubleArray should convert Some('1.23, 2.34') to Array[1.23, 2.34]") {
      val str = Some("1.23, 2.34")
      val doub: Array[Double] = Array(1.23, 2.34)
      assert(str.toDoubleArray(",").deep == doub.deep)
    }

    test("toDoubleArray should convert None to Array()") {
      val str: Option[String] = None
      assert(str.toDoubleArray(",").isEmpty)
    }

    test("GetOrFail on Some(String) should return String") {
      val str: Option[String] = Some("Peace")
      assert(str.getOrFail("Can't Get").equals("Peace"))
    }

    test("GetOrFail on None should Throw exception") {
      val str: Option[String] = None
      intercept[Exception]{
        str.getOrFail("Can't Get")
      }
    }
  }

  {
    import org.abyas.utils.implicits.TryImplicits.TryAndDefaultImplicityHelpers

    test("tryAndReturnDefault should return value if Success"){
      val tryItem: Try[String] = Success("Peace")
      assert(tryItem.tryAndReturnDefault("zzz").equals("Peace"))
    }

    test("tryAndReturnDefault should return default if Failure"){
      val tryItem: Try[String] = Try(None.get)
      assert(tryItem.tryAndReturnDefault("zzz").equals("zzz"))
    }

  }

}

package org.abyas.utils

import org.abyas.UnitTester
import org.abyas.utils.json.JsonHelpers
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

class TestJsonHelpers extends UnitTester {

  implicit val formats = DefaultFormats
  val jVal = JsonMethods.parse(scala.io.Source.fromFile("./src/test/resources/Test.json").reader())

  test("extractNestedJvalsExact: with path mapArrayStruct, key2 should retrieve Array[Value1, Value2]"){
    val item = JsonHelpers.extractNestedJvalsExact(jVal, Array("mapArrayStruct", "key2"))
    assert(item.extract[Array[String]].deep == Array("Value1", "Value2").deep)
  }

  test("extractNestedJvalsAll: with path key2 should retrieve Array[Value1, Value2]"){
    val item = JsonHelpers.extractNestedJvalsAll(jVal, Array("key2"))
    assert(item.children.map(_.values) == Seq(234, "Value", "Value1", "Value2"))
  }

}

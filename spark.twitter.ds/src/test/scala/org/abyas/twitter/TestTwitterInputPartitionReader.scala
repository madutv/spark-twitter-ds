package org.abyas.twitter

import org.abyas.UnitTester
import org.apache.spark.sql.types.DoubleType

import scala.collection.mutable.ListBuffer

class TestTwitterInputPartitionReader extends UnitTester {
  val listBuff: ListBuffer[Seq[Any]] = ListBuffer(
                          Seq(123, "ABC", Array("a", "b", "c")),
                          Seq(8910, "XYZ", Array("x", "y", "z")))

  val inputPart = new TwitterInputPartitionReader(listBuff)

  test("next should increment index"){
    assert(inputPart.next() == true)
  }

  test("get should get Internal Row") {
    val item = inputPart.get()
    (item.get(0, DoubleType) == 123.0)
  }

  test("next should return false since its passed lenght"){
    inputPart.next()
    assert(inputPart.next() == false)
  }

}

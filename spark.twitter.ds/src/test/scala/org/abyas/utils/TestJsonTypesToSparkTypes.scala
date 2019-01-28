package org.abyas.utils

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.abyas.UnitTester
import org.apache.spark.sql.types._
import com.fasterxml.jackson.databind.node.JsonNodeType._
import org.abyas.utils.json.JsonTypesToSparkTypes
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, MapData}


class TestJsonTypesToSparkTypes extends UnitTester {

  val mapper: ObjectMapper = new ObjectMapper()
  val actualObj: JsonNode = mapper.readTree(scala.io.Source.fromFile("./src/test/resources/Test.json").reader())


  test("matchJsonNodeToSparkType: LongType and Number should be converted to LongType"){
    val item = JsonTypesToSparkTypes.matchJsonNodeToSparkType(LongType, NUMBER, actualObj.get("num"))
    assert(item.isInstanceOf[java.lang.Long])
  }

  test("matchJsonNodeToSparkType: IntegerType and Number should be converted to IntegerType"){
    val item = JsonTypesToSparkTypes.matchJsonNodeToSparkType(IntegerType, NUMBER, actualObj.get("num"))
    assert(item.isInstanceOf[Integer])
  }

  test("matchJsonNodeToSparkType: Double and Number should be converted to DoubleType"){
    val item = JsonTypesToSparkTypes.matchJsonNodeToSparkType(DoubleType, NUMBER, actualObj.get("num"))
    assert(item.isInstanceOf[Double])
  }

  test("matchJsonNodeToSparkType: String and STRING should be converted to UTF8"){
    val item = JsonTypesToSparkTypes.matchJsonNodeToSparkType(StringType, STRING, actualObj.get("str"))
    assert(item.isInstanceOf[UTF8String])
  }

  test("matchJsonNodeToSparkType: ArraryType.LongType and ARRAY should be converted to ArrayData of LongType"){
    val item = JsonTypesToSparkTypes.matchJsonNodeToSparkType(ArrayType(LongType), ARRAY, actualObj.get("follow"))
    assert(item.isInstanceOf[ArrayData])
    assert(item.asInstanceOf[ArrayData].array.head.isInstanceOf[Long])
  }

  test("matchJsonNodeToSparkType: ArraryType.StringType and ARRAY should be converted to ArrayData of String"){
    val item = JsonTypesToSparkTypes.matchJsonNodeToSparkType(ArrayType(StringType), ARRAY, actualObj.get("track"))
    assert(item.isInstanceOf[ArrayData])
    assert(item.asInstanceOf[ArrayData].array.head.isInstanceOf[UTF8String])
  }


  test("matchJsonNodeToSparkType: MapType(StringType, DoubleTyep) and OBJECT should " +
    "be converted to MapType of String & Double"){
    val item = JsonTypesToSparkTypes.matchJsonNodeToSparkType(MapType(StringType, DoubleType),
      OBJECT, actualObj.get("mapObj"))
    assert(item.isInstanceOf[MapData])
    assert(item.asInstanceOf[ArrayBasedMapData].valueArray.array.head.isInstanceOf[Double])
    assert(item.asInstanceOf[ArrayBasedMapData].keyArray.array.head.isInstanceOf[UTF8String])
  }

  test("matchJsonNodeToSparkType: Struct and OBJECT should " +
    "be converted to InternalRow") {
    val struct = StructType(Array(StructField("key", DoubleType), StructField("key1", StringType)))
    val item = JsonTypesToSparkTypes.matchJsonNodeToSparkType(struct, OBJECT, actualObj.get("mapStruct"))
    assert(item.isInstanceOf[InternalRow])
    assert(item.asInstanceOf[InternalRow].get(0, DoubleType) == 124.0)
    assert(item.asInstanceOf[InternalRow].get(1, StringType).equals(UTF8String.fromString("Value")))
  }

  test("matchJsonNodeToSparkType: Array of Struct and OBJECT should " +
    "be converted to Array InternalRow") {
    val struct = ArrayType(StructType(Array(
          StructField("key", DoubleType),
          StructField("key1", StringType))))

    val item = JsonTypesToSparkTypes.matchJsonNodeToSparkType(struct, ARRAY, actualObj.get("mapArrayStruct"))
    assert(item.isInstanceOf[ArrayData])
    assert(item.asInstanceOf[ArrayData].array.head.asInstanceOf[InternalRow].get(0, DoubleType) == 124.0)
  }


}

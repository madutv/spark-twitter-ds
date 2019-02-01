package org.abyas.twitter

import java.util.Optional

import org.abyas.UnitTester
import org.apache.spark.sql.types._

class TestTwitterSchema extends UnitTester {
  val defaultStruct: StructType = StructType(Array(
    StructField("twitter", StringType)
  ))
  test("setSchema: sets Schema, SchemaCols & schemaTypes"){

    val struct: StructType = StructType(Array(
      StructField("name", StringType),
      StructField("age", IntegerType)))

    TwitterSchema.setSchema(Optional.empty())
    assert(TwitterSchema.Schema.equals(defaultStruct))
    assert(TwitterSchema.schemaColumns.deep == Array("twitter").deep)
    assert(TwitterSchema.schemaTypes.toArray.deep == Array(StringType).deep)
    TwitterSchema.setSchema(Optional.of(struct))
    assert(TwitterSchema.Schema.equals(struct))
    assert(TwitterSchema.schemaColumns.deep == Array("name", "age").deep)
    assert(TwitterSchema.schemaTypes.toArray.deep == Array(StringType, IntegerType).deep)
  }

  test("setRequestedColumns: sets cols"){
    val testCol: Array[Array[String]] = Array(Array("a", "b"), Array("x", "y", "z"))
    assert(TwitterSchema.cols.isEmpty)
    TwitterSchema.setRequestedColumns(testCol)
    assert(TwitterSchema.cols.deep == testCol.deep)
    TwitterSchema.setSchema(Optional.of(defaultStruct))
    TwitterSchema.setRequestedColumns(Array())
  }

}

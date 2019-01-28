package org.abyas.utils.json

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.fasterxml.jackson.databind.node.JsonNodeType.{ARRAY, BOOLEAN, NUMBER, OBJECT}

/**
  * Helps converting JsonNode types to Spark Types
  */
object JsonTypesToSparkTypes {

  /**
    * This function takes in a Spark StructType and a sequence of JsonNodes and
    * applies approriate conversions from JsonNodeType to Spark DataType.
    * For example, if the expected struct type is a MapType and the incomming
    * JsonNodeType is Object, Object and its underlyings are converted to
    * MapData. Similarly, if the expected type is StructType and incoming
    * JsonNodeType is Object, Object will be converted to InternalRow
    *
    * @param struct StructType to provide insight on conversions
    * @param jnodes Sequent of JsonNodeType. There is a one to one relation
    *               between elements in jnodes and StructField in StructType
    * @return jnodes converted to sequence of Spark Compatible types
    */
  def matchJsonNodesToSparkTypes(struct: StructType, jnodes: Seq[JsonNode]): Seq[Any] = {
    struct.fields.map(_.dataType).zip(jnodes).map(a => matchJsonNodeToSparkType(a._1, a._2.getNodeType, a._2))
  }

  /**
    * Convertes specific JsonNodeType to Spark Compatible types based on expected
    * DataType
    * @param sparkType. Spark DataType. This is a hint for conversion
    * @param jType. JsonNodeType.
    * @param jnode JsonNode
    * @return JsonNode converted to spark compatible type
    */
  def matchJsonNodeToSparkType(sparkType: DataType, jType: JsonNodeType, jnode: JsonNode): Any = {
    (sparkType, jnode.getNodeType, jnode) match {
      case(a: ArrayType, ARRAY, c) => jsonArrayToSparkArrayData(a, c)
      case(_, ARRAY, _) => throw new Exception("Json Array should be mapped to ArrayType")
      case(a: MapType, OBJECT, c) => jsonObjectToSparkMapData(a, c)
      case(a: StructType, OBJECT, c) => jsonObjectToSparkInteralRow(a, c)
      case(_, OBJECT, _) => throw new Exception("Json object should be mapped to MapType or StructType")
      case(LongType, NUMBER, c) => c.asLong()
      case(DoubleType, NUMBER, c) => c.asDouble()
      case(IntegerType, NUMBER, c) => c.asInt()
      case(BooleanType, BOOLEAN, c) => c.asBoolean()
      case(_, _, c) => UTF8String.fromString(c.asText)
    }
  }

  /**
    * Converts JsonNode ARRAY type to ArrayData if DataType is ArrayType
    * @param sparkType ArrayType
    * @param node JsonNode to convert
    * @return ArrayData
    */
  def jsonArrayToSparkArrayData(sparkType: ArrayType, node: JsonNode): Any = {
    val fields = node.elements()
    var arr: Array[Any] = Array()
    while(fields.hasNext){
      val field = fields.next()
      val itemVal: Any = matchJsonNodeToSparkType(sparkType.elementType, field.getNodeType, field)
      arr = arr.:+(itemVal)
    }
    ArrayData.toArrayData(arr)
  }

  /**
    * Converts JsonNodeType from OBJECT to ArrayBasedMapData
    * @param sparkType MapType
    * @param node JsonNode
    * @return ArrayBasedMapData which is compatible with Spark MapType
    */
  def jsonObjectToSparkMapData(sparkType: MapType, node: JsonNode): Any = {
    import scala.collection.mutable.Map
    type JavaMapIterator = java.util.Iterator[java.util.Map.Entry[String, JsonNode]]
    val fields: JavaMapIterator = node.fields()
    val mMap: Map[String, Any] = Map()
    while(fields.hasNext){
      val field = fields.next()
      mMap(field.getKey) = matchJsonNodeToSparkType(sparkType.valueType, field.getValue.getNodeType, field.getValue)
    }
    val keys: Array[UTF8String] = mMap.keys.toArray.map(a => UTF8String.fromString(a))
    val values: Array[Any] = mMap.values.toArray
    ArrayBasedMapData(keys, values)
  }

  /**
    * Converts JsonNodeType from OBJECT to InternalRow
    * @param sparkType: StructType
    * @param node. JsonNode
    * @return InternalRow
    */
  def jsonObjectToSparkInteralRow(sparkType: StructType, node: JsonNode): Any = {
    type JavaMapIterator = java.util.Iterator[JsonNode]
    var seq: Seq[JsonNode] = Seq()
    val fields: JavaMapIterator = node.elements()
    while(fields.hasNext){
      val field = fields.next()
      seq = seq.:+(field)
    }
    val valArr = matchJsonNodesToSparkTypes(sparkType, seq).toArray
    InternalRow(valArr: _*)
  }


}

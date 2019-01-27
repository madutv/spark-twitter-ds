package org.abyas.twitter

import java.util.Optional
import org.apache.spark.sql.types._


object TwitterSchema {

  var Schema: StructType = StructType(StructField("twitter", StringType) :: Nil)
  var schemaColumns: Array[String] = Schema.fieldNames
  var schemaTypes: Seq[DataType] = extractSchemaTypes().toSeq
  var cols: Array[Array[String]] = Array()

  def setSchema(oSchema: Optional[StructType]): Unit = {
    if(oSchema.isPresent){
      Schema = oSchema.get()
      schemaColumns = Schema.fieldNames
      schemaTypes = extractSchemaTypes()
    }
  }

  def extractSchemaTypes(): Array[DataType] = Schema.fields.map(_.dataType)

  def setRequestedColumns(columns: Array[Array[String]]): Unit = cols = columns

}

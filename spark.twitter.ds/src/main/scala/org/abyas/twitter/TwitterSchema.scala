package org.abyas.twitter

import java.util.Optional
import org.apache.spark.sql.types._


/**
  * Object to stores Twitter Schema details
  */
object TwitterSchema {

  var Schema: StructType = StructType(StructField("twitter", StringType) :: Nil)

  var schemaColumns: Array[String] = Schema.fieldNames
  var schemaTypes: Seq[DataType] = extractSchemaTypes().toSeq
  var cols: Array[Array[String]] = Array()

  /**
    * If schema is set during spark read, Schema attribute will be
    * overwritten with user specified StructType
    * @param oSchema
    */
  def setSchema(oSchema: Optional[StructType]): Unit = {
    if(oSchema.isPresent){
      Schema = oSchema.get()
      schemaColumns = Schema.fieldNames
      schemaTypes = extractSchemaTypes()
    }
  }

  /**
    * Extract Schema Types
    * @return
    */
  def extractSchemaTypes(): Array[DataType] = Schema.fields.map(_.dataType)

  /**
    * If columns are specified as option in spark read, this method will
    * simply set cols on this object to columns
    * @param columns
    */
  def setRequestedColumns(columns: Array[Array[String]]): Unit = cols = columns

}

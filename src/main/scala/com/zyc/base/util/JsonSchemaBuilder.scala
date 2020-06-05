package com.zyc.base.util

import java.util.regex.Pattern
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import scala.collection.mutable.ArrayBuffer

object JsonSchemaBuilder {
 
 
  final val columnSplitPattern = Pattern.compile("\\s*,\\s*")
  private final val fieldSplitPattern = Pattern.compile("\\.")
  private final val fieldPattern = Pattern.compile("([\\w\\.]+)(?:\\s+as\\s+\\w+)?")
  private final val DEFAULT_NULLABLE=true


  def getJsonSchema(schema: String): StructType = {
    getSchemaByFieldsList(columnSplitPattern.split(schema).map(getFieldList).toList)
  }
 
  private def getFieldList(singleField: String): List[String] = {
    val fieldMatch = fieldPattern.matcher(singleField)
    if (fieldMatch.matches()) {
      val fieldSource = fieldMatch.group(1)
      val fieldArray = fieldSplitPattern.split(fieldSource)
      fieldArray.toList
    } else {
      throw new IllegalArgumentException(s"field format error:$singleField ,we need parent.children(as aliasName)")
    }
  }
 
  private def getSchemaByFieldsList(fieldsList: List[List[String]]): StructType = {
    fieldsList.map(getStrcutType).reduce(mergeStructType)
  }
 
  private def getStrcutType(fields: List[String]): StructType = {
    fields match {
      case head :: Nil ⇒ StructType(StructField(head, StringType, DEFAULT_NULLABLE) :: Nil)
      case head :: tail ⇒ StructType(StructField(head, getStrcutType(tail), DEFAULT_NULLABLE) :: Nil)
    }
  }
 
  private def mergeStructType(left: StructType, right: StructType): StructType = {
    val newFields = ArrayBuffer.empty[StructField]
    val leftFields = left.fields
    val rightFields = right.fields
    val rightMapped = fieldsMap(rightFields)
    leftFields.foreach {
      case leftField@StructField(leftName, leftType, leftNullable, _) =>
        rightMapped.get(leftName)
          .map {
            case rightField@StructField(_, rightType, rightNullable, _) =>
              leftField.copy(
                dataType = mergeStructType(leftType.asInstanceOf[StructType], rightType.asInstanceOf[StructType]),
                nullable = leftNullable || rightNullable)
          }
          .orElse(Some(leftField))
          .foreach(newFields += _)
    }
 
    val leftMapped = fieldsMap(leftFields)
    rightFields
      .filterNot(f => leftMapped.get(f.name).nonEmpty)
      .foreach(newFields += _)
    StructType(newFields)
  }
 
  private def fieldsMap(fields: Array[StructField]): Map[String, StructField] = {
    import scala.collection.breakOut
    fields.map(s ⇒ (s.name, s))(breakOut)
  }
 
}
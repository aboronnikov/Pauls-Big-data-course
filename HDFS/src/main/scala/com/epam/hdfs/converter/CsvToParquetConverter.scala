package com.epam.hdfs.converter

import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

/**
 * A file that converts csv to parquet format.
 */
object CsvToParquetConverter {

  /**
   * Write a field in a csv file to a field in a group.
   *
   * @param group     group
   * @param value     value to write
   * @param fieldType type of the field, according to the schema
   * @param fieldName name of the field from the schema
   */
  private def writeCorrectGroupValue(group: Group, value: String, fieldType: PrimitiveTypeName, fieldName: String): Unit = {
    if (value.nonEmpty) {
      fieldType match {
        case PrimitiveTypeName.INT32                => group.append(fieldName, value.toInt)
        case PrimitiveTypeName.INT64                => group.append(fieldName, value.toLong)
        case PrimitiveTypeName.INT96                => group.append(fieldName, value)
        case PrimitiveTypeName.DOUBLE               => group.append(fieldName, value.toDouble)
        case PrimitiveTypeName.FLOAT                => group.append(fieldName, value.toFloat)
        case PrimitiveTypeName.BINARY               => group.append(fieldName, value)
        case PrimitiveTypeName.BOOLEAN              => group.append(fieldName, value.toBoolean)
        case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY => group.append(fieldName, value)
      }
    }
  }

  /**
   * Creates a group from a line in a csv file.
   *
   * @param line         line from the csv file
   * @param csvSeparator separator, that separates values in the csv file
   * @param schema       schema of the csv and the parquet files
   * @return group created from a line.
   */
  private def formGroupFromALine(line: String, csvSeparator: String, schema: MessageType): Group = {
    val group = new SimpleGroup(schema)
    val values = line.split(csvSeparator)
    for {id <- 0 until schema.getFieldCount} {
      val fieldName = schema.getColumns.get(id).getPrimitiveType.getName
      val fieldType = schema.getColumns.get(id).getPrimitiveType.getPrimitiveTypeName
      writeCorrectGroupValue(group, values(id), fieldType, fieldName)
    }
    group
  }

  /**
   * Lazily transforms the stream of lines from input into the stream of groups to be written to a file.
   *
   * @param stream       stream of lines.
   * @param csvSeparator separator used in csv.
   * @param schema       schema of data in csv.
   * @return a stream of groups to be written to a file.
   */
  def transformIntoGroupStream(stream: Iterator[String], csvSeparator: String, schema: MessageType): Iterator[Group] = {
    val numberOfLinesToSkip = 1
    stream
      .drop(numberOfLinesToSkip) // skip the first line
      .map(line => formGroupFromALine(line, csvSeparator, schema))
  }
}
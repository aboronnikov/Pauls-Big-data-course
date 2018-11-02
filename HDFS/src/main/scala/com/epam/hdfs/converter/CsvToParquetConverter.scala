package com.epam.hdfs.converter
import java.io.{File, IOException}
import java.nio.file.{Files, Paths}
import java.util.stream.{Collectors, IntStream}
import com.epam.hdfs.inputprocessor.ArgConstants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{MessageType, MessageTypeParser}
import scala.collection.immutable
import scala.io.Source

/**
 * A file that converts csv to parquet format.
 */
object CsvToParquetConverter {
  /**
   * Number of lines to skip, when processing the csv file.
   */
  val NumberOfLinesToSkip = 1

  /**
   * Helper function that reads schema from the schema file.
   *
   * @return schema, read from the specified file.
   */
  private def readSchema(schemaFilePath: String): String = {
    val fileStream = Files.lines(Paths.get(schemaFilePath))
    val result = fileStream.collect(Collectors.joining())
    fileStream.close()
    result
  }

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
    IntStream.range(0, schema.getFieldCount).forEach(id => {
      val fieldName = schema.getColumns.get(id).getPrimitiveType.getName
      val fieldType = schema.getColumns.get(id).getPrimitiveType.getPrimitiveTypeName
      writeCorrectGroupValue(group, values(id), fieldType, fieldName)
    })
    group
  }

  /**
   * The main function of this utility that converts csv to parquet format.
   */
  def convertAndSaveAsANewFile(argumentMap: immutable.Map[String, String]): Unit = {
    val schemaFilePath = argumentMap(ArgConstants.SchemaPathArg)
    val csvFilePath = argumentMap(ArgConstants.CsvPathArg)
    val newFilePath = argumentMap(ArgConstants.NewFilePathArg)
    val csvSeparator = argumentMap(ArgConstants.CsvSeparatorArg)

    val schema = MessageTypeParser.parseMessageType(readSchema(schemaFilePath))
    val config = new Configuration
    val path = new Path(newFilePath)
    val writeSupport = new GroupWriteSupport
    GroupWriteSupport.setSchema(schema, config)

    if (new File(newFilePath).exists()) {
      throw new IOException(newFilePath + ".parquet already exists.")
    }

    val writer = new ParquetWriter[Group](
      path,
      writeSupport,
      ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
      ParquetWriter.DEFAULT_BLOCK_SIZE,
      ParquetWriter.DEFAULT_PAGE_SIZE,
      ParquetWriter.DEFAULT_PAGE_SIZE,
      ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
      ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
      ParquetProperties.WriterVersion.PARQUET_1_0,
      config
    )

    val bufferedSource = Source.fromFile(csvFilePath)
    val fileStream = bufferedSource.getLines
    fileStream
      .drop(NumberOfLinesToSkip) // skip the first line
      .map(line => formGroupFromALine(line, csvSeparator, schema))
      .foreach(group => writer.write(group))
    bufferedSource.close()
    writer.close()
  }
}
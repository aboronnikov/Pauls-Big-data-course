package com.epam.hdfs.inputprocessor

import java.io.{File, IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.schema.{MessageType, MessageTypeParser}
import resource.managed
import scala.io.Source

/**
 * Class that provides I/O utilities, for reading and writing.
 */
object IOUtils {

  /**
   * Creates a new file and writes the stream of groups to it.
   *
   * @param groupStream data to be written to a parquet file.
   * @param newFilePath the path to the new file (name of the new file).
   * @param schema      schema of the data that will be written to the file.
   */
  def writeGroupsToFile(groupStream: Iterator[Group], newFilePath: String, schema: MessageType): Unit = {
    if (new File(newFilePath).exists()) {
      throw new IOException(newFilePath + ".parquet already exists.")
    }
    val path = new Path(newFilePath)
    val writeSupport = new GroupWriteSupport
    val config = new Configuration
    GroupWriteSupport.setSchema(schema, config)
    for {
      writer <- managed(
        new ParquetWriter[Group](
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
      )
    } {
      groupStream.foreach(group => writer.write(group))
    }
  }

  /**
   * Helper function that reads schema from the schema file.
   *
   * @return schema, read from the specified file.
   */
  def readSchemaFromFile(schemaFilePath: String): MessageType = {
    var schemaString = ""
    for {
      fileStream <- managed(Source.fromFile(schemaFilePath))
    } {
      schemaString = fileStream.mkString
    }
    MessageTypeParser.parseMessageType(schemaString)
  }
}

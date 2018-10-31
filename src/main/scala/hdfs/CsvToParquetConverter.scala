package hdfs

import java.io._
import java.nio.file.{Files, Paths}
import java.util.stream.{Collectors, IntStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.schema.MessageTypeParser
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

/**
  * A file that converts csv to parquet format.
  *
  * @param schemaFilePath the path to the schema file.
  * @param csvFilePath    the path to the csv file to read from.
  * @param newFilePath    the name of the new file that will be created.
  * @param csvSeparator   separator between csv row values.
  */
object CsvToParquetConverter {
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
    * The main function of this utility that converts csv to parquet format.
    */
  def convertAndSaveAsANewFile(schemaFilePath: String, csvFilePath: String, newFilePath: String, csvSeparator: String): Unit = {
    val schema = MessageTypeParser.parseMessageType(readSchema(schemaFilePath))
    val config = new Configuration
    val path = new Path(newFilePath)
    val writeSupport = new GroupWriteSupport
    GroupWriteSupport.setSchema(schema, config)

    if (new File(newFilePath).exists()) {
      throw new IOException(newFilePath + ".parquet already exists")
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

    val fileStream = Files.lines(Paths.get(csvFilePath))// skip the first line
    fileStream.skip(1).forEach(line => {
      val group = new SimpleGroup(schema)
      val values = line.split(csvSeparator)
      IntStream.range(0, schema.getFieldCount).forEach(id => {
        val fieldName = schema.getColumns.get(id).getPrimitiveType.getName
        val fieldType = schema.getColumns.get(id).getPrimitiveType.getPrimitiveTypeName
        if (!values(id).isEmpty) {
          fieldType match {
            case PrimitiveTypeName.INT32                => group.append(fieldName, values(id).toInt)
            case PrimitiveTypeName.INT64                => group.append(fieldName, values(id).toLong)
            case PrimitiveTypeName.INT96                => group.append(fieldName, values(id))
            case PrimitiveTypeName.DOUBLE               => group.append(fieldName, values(id).toDouble)
            case PrimitiveTypeName.FLOAT                => group.append(fieldName, values(id).toFloat)
            case PrimitiveTypeName.BINARY               => group.append(fieldName, values(id))
            case PrimitiveTypeName.BOOLEAN              => group.append(fieldName, values(id).toBoolean)
            case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY => group.append(fieldName, values(id))
          }
        }
      })
      writer.write(group)
    })
    fileStream.close()
    writer.close()
  }
}

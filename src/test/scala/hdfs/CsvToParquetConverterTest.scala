package hdfs

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}

import org.junit.{After, Assert, Before, Test}
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.scalatest.junit.JUnitSuite


/**
  * This is a set of unit tests for test.CsvToParquetConverter.
  */
class CsvToParquetConverterTest extends JUnitSuite {

  private val CSV_FILE_NAME = "csv_test_file.temp"
  private val SCHEMA_FILE_NAME = "schema_test_file.temp"
  private val PARQUET_FILE_NAME = "parquet_test_file.parquet"
  private val PARQUET_AUX_FILE_NAME = ".parquet_test_file.parquet.crc"

  /**
    * Creates a csv file with some data.
    */
  private def setUpCsv(): Unit = {
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(CSV_FILE_NAME)))
    writer.write(
      "left,right\n" +
      "l,r")
    writer.close()
  }

  /**
    * Creates a parquet schema for the csv file from above.
    */
  private def setUpSchema(): Unit = {
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(SCHEMA_FILE_NAME)))
    writer.write(
      "message Schema {\n" +
      "required binary left (UTF8);\n" +
      "required binary right (UTF8);\n" +
      "}"
    )
    writer.close()
  }

  /**
    * Sets up the preliminaries.
    */
  @Before
  def setUp(): Unit = {
    setUpCsv()
    setUpSchema()
  }

  /**
    * Cleans up after itself by removing all the files that were created while executing unit tests.
    */
  @After
  def tearDown(): Unit = {
    val csvFile = new File(CSV_FILE_NAME)
    val schemaFile = new File(SCHEMA_FILE_NAME)
    val parquetFile = new File(PARQUET_FILE_NAME)
    val parquetAuxFile = new File(PARQUET_AUX_FILE_NAME)

    csvFile.delete()
    schemaFile.delete()
    parquetFile.delete()
    parquetAuxFile.delete()
  }

  /**
    * The unit test, that tests if test.CsvToParquetConverter successfully converts csv to parquet.
    */
  @Test
  def convertAndSaveAsANewFileTest(): Unit = {
    CsvToParquetConverter.convertAndSaveAsANewFile(SCHEMA_FILE_NAME, CSV_FILE_NAME, PARQUET_AUX_FILE_NAME, ",")
    val readSupport = new GroupReadSupport
    val path = new Path(PARQUET_FILE_NAME)
    val reader = new ParquetReader[Group](path, readSupport)
    val result = reader.read()
    Assert.assertNotNull(result)
    Assert.assertEquals(result.getString("left", 0), "l")
    Assert.assertEquals(result.getString("right", 0), "r")
    Assert.assertNull(reader.read())
  }
}

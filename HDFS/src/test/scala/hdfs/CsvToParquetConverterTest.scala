package hdfs
import java.io._

import com.epam.hdfs.converter.CsvToParquetConverter
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.StringUtils
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{MessageType, MessageTypeParser}
import org.junit.{After, Assert, Before, Test}
import org.scalatest.junit.JUnitSuite


/**
 * This is a set of unit tests for test.CsvToParquetConverter.
 */
class CsvToParquetConverterTest extends JUnitSuite {

  private val CsvFileName = "csv_test_file.temp"
  private val SchemaFileName = "schema_test_file.temp"
  private val ParquetFileName = "parquet_test_file.parquet"
  private val ParquetAuxiliaryFileName = ".parquet_test_file.parquet.crc"
  private val AlreadyExistingParquetFile = "ialreadyexist.parquet"

  /**
   * Creates a csv file with some data.
   */
  private def setUpCsv(): Unit = {
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(CsvFileName)))
    writer.write(
      "left,right, id\n" +
        "l,r,32")
    writer.close()
  }

  /**
   * Creates a parquet schema for the csv file from  above.
   */
  private def setUpSchema(): Unit = {
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(SchemaFileName)))
    writer.write(
      "message Schema {\n" +
        "required binary left (UTF8);\n" +
        "required binary right (UTF8);\n" +
        "required int32 id;\n" +
        "}"
    )
    writer.close()
  }

  /**
   * Sets up the file that will exist when tryToConvertWhenFileAlreadyExistsTest is called.
   */
  private def setUpExistingParquet(): Unit = {
    val file = new File(AlreadyExistingParquetFile)
    file.createNewFile()
  }

  /**
   * Sets up the preliminaries.
   */
  @Before
  def setUp(): Unit = {
    setUpExistingParquet()
    setUpCsv()
    setUpSchema()
  }

  /**
   * Cleans up after itself by removing all the files that were created while executing unit tests.
   */
  @After
  def tearDown(): Unit = {
    val csvFile = new File(CsvFileName)
    val schemaFile = new File(SchemaFileName)
    val parquetFile = new File(ParquetFileName)
    val parquetAuxFile = new File(ParquetAuxiliaryFileName)
    val existingParquetFile = new File(AlreadyExistingParquetFile)

    csvFile.delete()
    schemaFile.delete()
    parquetFile.delete()
    parquetAuxFile.delete()
    existingParquetFile.delete()
  }

  /**
   * The unit test, that tests if test.CsvToParquetConverter successfully converts csv to parquet.
   */
  @Test
  def convertAndSaveAsANewFileTest(): Unit = {
    CsvToParquetConverter.convertAndSaveAsANewFile(CsvFileName, StringUtils.COMMA_STR, ParquetFileName, SchemaFileName)
    val readSupport = new GroupReadSupport
    val path = new Path(ParquetFileName)
    val reader = new ParquetReader[Group](path, readSupport)
    val result = reader.read()
    Assert.assertNotNull(result)
    Assert.assertEquals(result.getString("left", 0), "l")
    Assert.assertEquals(result.getString("right", 0), "r")
    Assert.assertEquals(result.getInteger("id", 0), 32)
    Assert.assertNull(reader.read())
  }

  /**
   * Tests the case when the user enters the name of an existing file.
   */
  @Test(expected = classOf[IOException])
  def tryToConvertWhenFileAlreadyExistsTest(): Unit = {
    CsvToParquetConverter.convertAndSaveAsANewFile(CsvFileName, StringUtils.COMMA_STR, AlreadyExistingParquetFile, SchemaFileName)
  }

  /**
    * Parquet schema used for unit tests.
    */
  val Schema = MessageTypeParser.parseMessageType(
    "message Schema {\n" +
      "required binary left (UTF8);\n" +
      "required binary right (UTF8);\n" +
      "required int32 id;\n" +
      "}"
  )

  /**
    * Test for CsvToParquetConverter.formGroupFromALine.
    * Tests the method using reflection.
    */
  @Test
  def formGroupFromALineTest(): Unit = {
    val method = CsvToParquetConverter
      .getClass
      .getDeclaredMethod("formGroupFromALine", classOf[String], classOf[String], classOf[MessageType])
    method.setAccessible(true)
    val group = method.invoke(CsvToParquetConverter, "l,r,4", StringUtils.COMMA_STR, Schema).asInstanceOf[Group]

    val expected = List("l", "r", 4)
    val actual = List(group.getString("left", 0), group.getString("right", 0), group.getInteger("id", 0))
    Assert.assertEquals(expected, actual)
  }

  /**
    * Test for CsvToParquetConverter.transformIntoGroupStream.
    * Tests the method using reflection.
    */
  @Test
  def transformIntoGroupStreamTest(): Unit = {
    val group1 = new SimpleGroup(Schema)
    group1.add("left", "l")
    group1.add("right", "r")
    group1.add("id", 3)

    val group2 = new SimpleGroup(Schema)
    group2.add("left", "l")
    group2.add("right", "r")
    group2.add("id", 4)

    val expected = Array[AnyRef](group1.toString, group2.toString)

    val stream = Array("skip", "l,r,3", "l,r,4").toIterator

    val method = CsvToParquetConverter
      .getClass
      .getDeclaredMethod("transformIntoGroupStream", classOf[Iterator[String]], classOf[String], classOf[MessageType])

    method.setAccessible(true)
    val actual = method.invoke(CsvToParquetConverter, stream, StringUtils.COMMA_STR, Schema)
      .asInstanceOf[Iterator[Group]]
      .map(_.toString)
      .toArray[AnyRef]

    Assert.assertArrayEquals(expected, actual)
  }

  /**
    * Test for CsvToParquetConverter.writeCorrectGroupValue.
    * Tests the method using reflection.
    */
  @Test
  def writeCorrectGroupValueTest(): Unit = {
    val actualGroup = new SimpleGroup(Schema)
    val expectedGroup = new SimpleGroup(Schema)

    expectedGroup.append("left", "l")
    expectedGroup.append("right", "r")
    expectedGroup.append("id", 11)

    val method = CsvToParquetConverter
      .getClass
      .getDeclaredMethod("writeCorrectGroupValue", classOf[Group], classOf[String], classOf[PrimitiveTypeName], classOf[String])
    method.setAccessible(true)

    method.invoke(CsvToParquetConverter, actualGroup, "l", PrimitiveTypeName.BINARY, "left")
    method.invoke(CsvToParquetConverter, actualGroup, "r", PrimitiveTypeName.BINARY, "right")
    method.invoke(CsvToParquetConverter, actualGroup, "11", PrimitiveTypeName.INT32, "id")

    Assert.assertEquals(expectedGroup.toString, actualGroup.toString)
  }
}
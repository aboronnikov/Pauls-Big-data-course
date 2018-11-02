package hdfs
import java.io._
import hdfs.converter.CsvToParquetConverter
import hdfs.inputprocessor.{ArgConstants, ExecutionCaseConstants, Runner}
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.junit.{After, Assert, Before, Test}
import org.scalatest.junit.JUnitSuite


/**
 * This is a set of unit tests for test.CsvToParquetConverter.
 */
class CsvToParquetConverterTest extends JUnitSuite {

  private val CSV_FILE_NAME = "csv_test_file.temp"
  private val SCHEMA_FILE_NAME = "schema_test_file.temp"
  private val PARQUET_FILE_NAME = "parquet_test_file.parquet"
  private val PARQUET_AUX_FILE_NAME = ".parquet_test_file.parquet.crc"
  private val ALREADY_EXISTING_PARQUET_FILE = "ialreadyexist.parquet"

  /**
   * Map for convertAndSaveAsANewFileTest.
   */
  private val workingArgumentMap = Map(
    ArgConstants.SchemaPathArg -> SCHEMA_FILE_NAME,
    ArgConstants.CsvPathArg -> CSV_FILE_NAME,
    ArgConstants.NewFilePathArg -> PARQUET_FILE_NAME,
    ArgConstants.CsvSeparatorArg -> ","
  )

  /**
   * Map for tryToConvertWhenFileAlreadyExistsTest.
   */
  private val exceptionalArgumentMap = Map(
    ArgConstants.SchemaPathArg -> SCHEMA_FILE_NAME,
    ArgConstants.CsvPathArg -> CSV_FILE_NAME,
    ArgConstants.NewFilePathArg -> ALREADY_EXISTING_PARQUET_FILE,
    ArgConstants.CsvSeparatorArg -> ","
  )

  /**
   * Creates a csv file with some data.
   */
  private def setUpCsv(): Unit = {
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(CSV_FILE_NAME)))
    writer.write(
      "left,right, id\n" +
        "l,r,32")
    writer.close()
  }

  /**
   * Creates a parquet schema for the csv file from  above.
   */
  private def setUpSchema(): Unit = {
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(SCHEMA_FILE_NAME)))
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
    val file = new File(ALREADY_EXISTING_PARQUET_FILE)
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
    val csvFile = new File(CSV_FILE_NAME)
    val schemaFile = new File(SCHEMA_FILE_NAME)
    val parquetFile = new File(PARQUET_FILE_NAME)
    val parquetAuxFile = new File(PARQUET_AUX_FILE_NAME)
    val existingParquetFile = new File(ALREADY_EXISTING_PARQUET_FILE)

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
    CsvToParquetConverter.convertAndSaveAsANewFile(workingArgumentMap)
    val readSupport = new GroupReadSupport
    val path = new Path(PARQUET_FILE_NAME)
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
    CsvToParquetConverter.convertAndSaveAsANewFile(exceptionalArgumentMap)
  }

  /**
   * Tests the case when the user enters everything correctly.
   */
  @Test
  def checkArgumentsForNormalCaseTest(): Unit = {
    val normalCase =
      Runner.checkForNormalCase(
        Array(
          ArgConstants.NewFilePathArg + ArgConstants.KeyValueArgSeparator + PARQUET_FILE_NAME,
          ArgConstants.CsvSeparatorArg + ArgConstants.KeyValueArgSeparator + ",",
          ArgConstants.CsvPathArg + ArgConstants.KeyValueArgSeparator + CSV_FILE_NAME,
          ArgConstants.SchemaPathArg + ArgConstants.KeyValueArgSeparator + SCHEMA_FILE_NAME
        )
      )
    Assert.assertEquals(ExecutionCaseConstants.NormalCase, normalCase._1)
    Assert.assertTrue(normalCase._2.nonEmpty)
  }

  /**
   * Tests the case when the user enters gibberish arguments.
   */
  @Test
  def checkArgumentsForBadCaseTest(): Unit = {
    val badCase =
      Runner.checkForNormalCase(
        Array(
          "Bad",
          "Worse",
          "Worst"
        )
      )
    Assert.assertEquals(ExecutionCaseConstants.HelpOrBadArgsCase, badCase._1)
    Assert.assertTrue(badCase._2.isEmpty)
  }

  /**
   * Tests the case when the user enters -help
   */
  @Test
  def checkArgumentsForHelpCaseTest(): Unit = {
    val helpCase =
      Runner.checkForNormalCase(
        Array("-help")
      )
    Assert.assertEquals(ExecutionCaseConstants.HelpOrBadArgsCase, helpCase._1)
    Assert.assertTrue(helpCase._2.isEmpty)
  }
}
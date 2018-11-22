package hdfs

import com.epam.hdfs.converter.CsvToParquetConverter
import org.apache.hadoop.util.StringUtils
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.schema.{MessageType, MessageTypeParser}
import org.junit.{Assert, Test}
import org.scalatest.junit.JUnitSuite


/**
 * This is a set of unit tests for test.CsvToParquetConverter.
 */
class CsvToParquetConverterTest extends JUnitSuite {

  /**
   * Test of the main method in CsvToParquetConverter.
   */
  @Test
  def transformIntoGroupStreamTest(): Unit = {
    val group1 = new SimpleGroup(Schema)
    group1.add("left", "l")
    group1.add("right", "r")
    group1.add("dbl", 3.44)
    group1.add("test", true)
    group1.add("id", 3)
    group1.add("longId", 3L)

    val group2 = new SimpleGroup(Schema)
    group2.add("left", "l")
    group2.add("right", "r")
    group2.add("dbl", 3.55)
    group2.add("test", false)
    group2.add("id", 4)
    group2.add("longId", 4L)

    val expected = Array[AnyRef](group1.toString, group2.toString)

    val stream = Array("skip", "l,r,3.44,true,3,3", "l,r,3.55,false,4,4").toIterator

    val actual = CsvToParquetConverter.transformIntoGroupStream(stream, StringUtils.COMMA_STR, Schema)
      .asInstanceOf[Iterator[Group]]
      .map(_.toString)
      .toArray[AnyRef]

    Assert.assertArrayEquals(expected, actual)
  }

  /**
   * Parquet schema used for unit tests.
   */
  val Schema: MessageType = MessageTypeParser.parseMessageType(
    "message Schema {\n" +
      "required binary left (UTF8);\n" +
      "required binary right (UTF8);\n" +
      "required double dbl;\n" +
      "required boolean test;\n" +
      "required int32 id;\n" +
      "required int64 longId;\n" +
      "}"
  )
}
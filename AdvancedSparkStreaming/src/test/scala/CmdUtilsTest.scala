import com.epam.utilities.CmdUtils
import org.apache.commons.cli.ParseException
import org.junit.Test
import org.scalatest.junit.JUnitSuite

/**
  * Tests for the CmdUtils class.
  */
class CmdUtilsTest extends JUnitSuite {

  /**
    * Test for the parse method.
    */
  @Test(expected = classOf[ParseException])
  def parseTest(): Unit = {
    val cmdArgs = Array(
      "-inputTopic", "test",
      "-outputTopic", "test2",
      "-bootstrapServer", "www.test.com",
      // "-startingOffsets", "earliest",
      "-checkPointLocation", "bla"
    )
    CmdUtils.parse(cmdArgs)
  }

  /**
    * Another test for the parse method.
    */
  @Test
  def parseTest1(): Unit = {
    val cmdArgs = Array(
      "-inputTopic", "test",
      "-outputTopic", "test2",
      "-bootstrapServer", "www.test.com",
      "-startingOffsets", "earliest",
      "-checkPointLocation", "bla"
    )
    CmdUtils.parse(cmdArgs)
  }
}

import com.Task1.config
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.util.regex.Pattern

class Test1 extends AnyFlatSpec with Matchers{

  val conf: Config = ConfigFactory.load()
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  it should "Match the log pattern defined in the conf file" in {
    val example_msg = "12:47:01.530 [scala-execution-context-global-13] ERROR - /US|e`bzLKW#8\\W1_:Az'Yc{d~"
    val pattern = Pattern.compile(config.getString("mr.log_pattern"))
    val matcher = pattern.matcher(example_msg)
    matcher.find() shouldBe(true)
  }

  it should "Match the log pattern defined in the conf file and extract the right pattern for time" in {
    val example_msg = "12:47:01.530 [scala-execution-context-global-13] ERROR - /US|e`bzLKW#8\\W1_:Az'Yc{d~"
    val pattern = Pattern.compile(config.getString("mr.log_pattern"))
    val matcher = pattern.matcher(example_msg)
    matcher.find()
    val time = matcher.group(1)
    assert(time.equals("12:47:01.530 "))
  }

  it should "Match the log pattern defined in the conf file and extract the right pattern for message type" in {
    val example_msg = "12:47:01.530 [scala-execution-context-global-13] ERROR - /US|e`bzLKW#8\\W1_:Az'Yc{d~"
    val pattern = Pattern.compile(config.getString("mr.log_pattern"))
    val matcher = pattern.matcher(example_msg)
    matcher.find()
    val time = matcher.group(2)
    assert(time.equals("ERROR"))
  }

  it should "Match the log pattern defined in the conf file and extract the right pattern for actual log message" in {
    val example_msg = "12:47:01.530 [scala-execution-context-global-13] ERROR - /US|e`bzLKW#8\\W1_:Az'Yc{d~"
    val pattern = Pattern.compile(config.getString("mr.log_pattern"))
    val matcher = pattern.matcher(example_msg)
    matcher.find()
    val time = matcher.group(3)
    assert(time.equals("/US|e`bzLKW#8\\W1_:Az'Yc{d~"))
  }

  it should "Match the message pattern defined in the conf file " in {
    val example_msg = "B.oB8V>DFL&q]qm}\\$}8OQ7mbe3X7wG8nK5fh$zmw2K2eNwT05:,SR9'C"
    val pattern = Pattern.compile(config.getString("mr.detect_pattern"))
    val matcher = pattern.matcher(example_msg)
    matcher.find() shouldBe(true)
  }

  it should  "Not match the message pattern defined in the conf file " in {
    val example_msg = "/US|e`bzLKW#8\\W1_:Az'Yc{d~"
    val pattern = Pattern.compile(config.getString("mr.detect_pattern"))
    val matcher = pattern.matcher(example_msg)
    matcher.find() shouldBe(false)
  }
}

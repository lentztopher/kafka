package kafka.controller

import java.util.Properties

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{Logging, TestUtils}
import kafka.common.KafkaException
import kafka.utils.TestUtils.waitUntilTrue
import org.apache.log4j.Logger
import org.junit.Assert._
import org.junit.Test

class IneligibleControllerFailoverTest extends KafkaServerTestHarness with Logging {
  val log = Logger.getLogger(classOf[IneligibleControllerFailoverTest])
  val numNodes = 2
  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.LeaderIneligibleBrokerIdProp, (numNodes-1).toString)

  override def generateConfigs() = TestUtils.createBrokerConfigs(numNodes, zkConnect)
    .map(KafkaConfig.fromProps(_, overridingProps))

  @Test
  def testIneligibleControllerNotElected() {
    val validController = Some(0)
    assertEquals("Ineligible controller elected", validController, getControllerId())

    val eligibleBroker = servers.find( _.config.brokerId == 0 ).get
    eligibleBroker.shutdown()
    eligibleBroker.awaitShutdown()

    assertEquals("Ineligible controller elected", None, getControllerId())

    eligibleBroker.startup()

    waitUntilTrue(condition = () => { getControllerId() == validController },
      msg = "Controller did not recover", waitTime = 15000)
  }

  def getControllerId(): Option[Int] = {
    try {
      return Some(zkUtils.getController())
    } catch {
      case e: KafkaException => return None
    }
  }
}

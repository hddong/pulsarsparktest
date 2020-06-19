import org.apache.log4j.{Level, Logger}
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData
import org.apache.pulsar.spark.SparkStreamingPulsarReceiver
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author hongdd
 * @description
 * @since 2020-06-19 09:48
 */
object SparkTest {
  val URL = "pulsar://10.139.17.60:6650/"
  val TOPIC = "persistent://public/default/test"

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().master("local[2]").appName("test").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val consConf = new ConsumerConfigurationData[Array[Byte]]()
    val topics = new java.util.HashSet[String]()
    topics.add(TOPIC)
    consConf.setTopicNames(topics)
    consConf.setSubscriptionName("sub")
    val stream = ssc.receiverStream(new SparkStreamingPulsarReceiver(URL, consConf, new AuthenticationDisabled))
    stream.map(println)
    stream.count().print()
    ssc.start()
    ssc.awaitTermination()
  }
}

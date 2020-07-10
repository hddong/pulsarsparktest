package org.apache.spark.sql.pulsar

import java.util.UUID
import java.{util => ju}

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.pulsar.PulsarConfigurationUtils.{clientConfKeys, readerConfKeys}
import org.apache.spark.sql.pulsar.PulsarOptions.{PULSAR_CLIENT_OPTION_KEY_PREFIX, PULSAR_READER_OPTION_KEY_PREFIX, SERVICE_URL_OPTION_KEY}
import org.apache.spark.sql.pulsar.PulsarProvider.{paramsToPulsarConf}

/**
 * @author hongdd
 * @description
 * @since 2020-06-19 10:00
 */
object PulsarRddTest {
  val URL = "pulsar://10.139.17.60:6650/"
  val ADMIN_URL= "http://10.139.17.60:8080"
  val TOPIC = "persistent://public/default/test"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("test").getOrCreate()
    val map = Map("service.url" -> URL, "admin.url" -> ADMIN_URL, "topic" -> TOPIC)

    val metadataPath = "/tmp/data"
    val confs = prepareConfForReader(map)
    val subscriptionNamePrefix = s"spark-pulsar-${UUID.randomUUID}-${metadataPath.hashCode}"

    val end = AdminUtils.getLatestOffsets(Set(TOPIC), ADMIN_URL)
    val start = AdminUtils.getEarliestOffsets(Set(TOPIC), URL)
    val offsetRanges = end.keySet.map { tp =>
      val untilOffset = end(tp)
      val startOffset = start(tp)
      PulsarOffsetRange(tp, startOffset, untilOffset, None)
    }.toSeq

    val rdd = new PulsarSourceRDD(spark.sparkContext, new SchemaInfoSerializable(SchemaUtils.emptySchemaInfo()),
      confs._1, confs._2, offsetRanges,
      pollTimeoutMs(map), false, subscriptionNamePrefix, jsonOptions)

    rdd.collect().foreach(println)
    println("-=-----------")
    spark.stop()
  }

  private def jsonOptions: JSONOptionsInRead = {
    val spark = SparkSession.getActiveSession.get
    new JSONOptionsInRead(
      CaseInsensitiveMap(Map.empty),
      spark.sessionState.conf.sessionLocalTimeZone,
      spark.sessionState.conf.columnNameOfCorruptRecord)
  }

  private def pollTimeoutMs(caseInsensitiveParams: Map[String, String]): Int =
    caseInsensitiveParams
      .getOrElse(
        PulsarOptions.POLL_TIMEOUT_MS,
        (SparkEnv.get.conf.getTimeAsSeconds("spark.network.timeout", "120s") * 1000).toString)
      .toInt

  private def prepareConfForReader(parameters: Map[String, String])
  : (ju.Map[String, Object], ju.Map[String, Object], String, String) = {


    var clientParams = getClientParams(parameters)
    clientParams += (SERVICE_URL_OPTION_KEY -> URL)
    val readerParams = getReaderParams(parameters)

    (
      paramsToPulsarConf("pulsar.client", clientParams),
      paramsToPulsarConf("pulsar.reader", readerParams),
      URL,
      ADMIN_URL
    )
  }

  private def getClientParams(parameters: Map[String, String]): Map[String, String] = {
    val lowercaseKeyMap = parameters.keySet
      .filter(_.startsWith(PULSAR_CLIENT_OPTION_KEY_PREFIX))
      .map { k =>
        k.drop(PULSAR_CLIENT_OPTION_KEY_PREFIX.length).toString -> parameters(k)
      }
      .toMap
    lowercaseKeyMap.map { case (k, v) =>
      clientConfKeys.getOrElse(
        k, throw new IllegalArgumentException(s"$k not supported by pulsar")) -> v
    }
  }

  private def getReaderParams(parameters: Map[String, String]): Map[String, String] = {
    val lowercaseKeyMap = parameters.keySet
      .filter(_.startsWith(PULSAR_READER_OPTION_KEY_PREFIX))
      .map { k =>
        k.drop(PULSAR_READER_OPTION_KEY_PREFIX.length).toString -> parameters(k)
      }
      .toMap
    lowercaseKeyMap.map { case (k, v) =>
      readerConfKeys.getOrElse(
        k, throw new IllegalArgumentException(s"$k not supported by pulsar")) -> v
    }
  }
}

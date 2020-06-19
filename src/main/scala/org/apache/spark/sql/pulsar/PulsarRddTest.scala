package org.apache.spark.sql.pulsar

import org.apache.spark.sql.SparkSession

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
    new PulsarProvider().createSource(
      spark.sqlContext, "/tmp/data", Option.empty, "providerName", map)
    spark.stop()
  }
}

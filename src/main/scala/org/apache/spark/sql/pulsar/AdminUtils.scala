/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.pulsar


import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.{MessageId, PulsarClient}
import org.apache.spark.sql.pulsar.PulsarOptions.PARTITION_SUFFIX
import org.apache.spark.util.Utils

object AdminUtils {

  def getEarliestOffsets(topics: Set[String], serviceUrl: String): Map[String, MessageId] = {
    val client = PulsarClient
      .builder()
      .serviceUrl(serviceUrl)
      .build()
    val t2id = topics.map { tp =>
      val reader = client.newReader().startMessageId(MessageId.earliest).topic(tp).create()
      val mid = reader.readNext().getMessageId
      reader.close()
      (tp, mid)
    }.toMap
    client.close()
    t2id
  }

  def getLatestOffsets(topics: Set[String], adminUrl: String): Map[String, MessageId] = {
    Utils.tryWithResource(PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) { admin =>
      topics.flatMap { tp =>
        val partNum = admin.topics().getPartitionedTopicMetadata(tp).partitions
        if (partNum > 1) {
          (0 until partNum).map { pn =>
            (
              s"$tp$PARTITION_SUFFIX$pn",
              PulsarSourceUtils.seekableLatestMid(
                admin.topics().getLastMessageId(s"$tp$PARTITION_SUFFIX$pn")))
          }
        } else {
          (tp, PulsarSourceUtils.seekableLatestMid(admin.topics().getLastMessageId(tp))) :: Nil
        }
      }.toMap
    }
  }
}

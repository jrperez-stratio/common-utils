/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.common.utils.integration

import com.stratio.common.utils.components.config.impl.TypesafeConfigComponent
import com.stratio.common.utils.components.logger.impl.Slf4jLoggerComponent
import com.stratio.common.utils.components.transaction_manager.TransactionResource
import com.stratio.common.utils.components.transaction_manager.impl.ZookeeperRepositoryWithTransactionsComponent

object ZKTransactionTestClient extends App {

  case class OutputEntry(client: String, resources: Seq[String], segment: Int, part: Int) {
    override def toString: String =
      s"client=$client resources=[${resources.mkString(", ")}] segment=$segment part=$part"
  }

  object OutputEntry {

    def apply(line: String): OutputEntry = {
      //TODO: Be able to extract more than one resource id
      val ExtractionRegex =
        """^client=(\w+)\s+resources=\[\s*((?:\w+)?(?:,\s*\w+\s*)*)\s*]\s+segment=(\d+)\s+part=(\d+)\s*$""".r
      line match {
        case ExtractionRegex(client, resources, segmentStr, partStr) =>
          OutputEntry(client, resources.split(",").map(_.trim), segmentStr.toInt, partStr.toInt)
      }
    }

  }

  case class Resource(id: String) extends TransactionResource

  val usageMsg =
    "Usage: ZKTransactionTestClient <client_label> <nresources> [resourceid] [<no_segment_parts> <part_duration>]"

  require(args.length > 2, usageMsg)

  val Array(
    label,
    nResourcesStr,
    remainingArgs @ _*
  ) = args

  require(nResourcesStr.toInt <= remainingArgs.size)

  val (resourcesStr, segmentsStr) = remainingArgs.splitAt(nResourcesStr.toInt)
  val resources = resourcesStr map Resource

  require((remainingArgs.size - resources.size) % 2 == 0, usageMsg)

  val segments = segmentsStr.grouped(2)


  def mayBeProtected(block: => Unit): Unit = {
    def nestedAtomicAreas(resources: Seq[TransactionResource]): Unit =
      if(resources.isEmpty) block
      else transactionManager.repository.atomically("test", resources.head) {
        nestedAtomicAreas(resources.tail)
      }
    nestedAtomicAreas(resources)
  }

  val transactionManager = new ZookeeperRepositoryWithTransactionsComponent
    with TypesafeConfigComponent with Slf4jLoggerComponent

  segments.zipWithIndex foreach { case (segment, iteration) =>
    val Seq(nParts, millis) = segment.map(_.toLong)
    mayBeProtected {
      (1L to nParts) foreach { part =>
        println(OutputEntry(label, resources.map(_.id), iteration, part.toInt))
        Thread.sleep(millis)
      }
    }
  }

}


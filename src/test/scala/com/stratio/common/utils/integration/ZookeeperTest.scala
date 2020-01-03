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
import com.stratio.common.utils.components.dao.GenericDAOComponent
import com.stratio.common.utils.components.logger.impl.Slf4jLoggerComponent
import com.stratio.common.utils.components.repository.impl.ZookeeperRepositoryComponent
import com.stratio.common.utils.integration.ZKTransactionTestClient.OutputEntry
import org.apache.curator.test.TestingServer
import org.apache.curator.utils.CloseableUtils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.util.{Failure, Success}

@RunWith(classOf[JUnitRunner])
class ZookeeperIntegrationTest extends WordSpec
  with Matchers
  with BeforeAndAfterAll {

  val ZookeeperTestPort: Int = 10666

  var zkTestServer: TestingServer = _

  def zookeeperStart(): Unit = {
    zkTestServer = new TestingServer(ZookeeperTestPort)
    zkTestServer.start()
  }

  def zookeeperStop(): Unit = {
    CloseableUtils.closeQuietly(zkTestServer)
    zkTestServer.stop()
  }

  override def beforeAll {
    zookeeperStart()
  }

  override def afterAll {
    zookeeperStop()
  }

  "A dao component" should {

    val component = new DummyZookeeperDAOComponent
    import component.dao

    /*"save a dummy in ZK and get it" in {
      dao.create("test1", Dummy("value"))
      dao.get("test1") should be(Success(Some(Dummy("value"))))
    }

    "update the dummy in ZK and get it" in {
      dao.update("test1", Dummy("newValue"))
      dao.get("test1") should be(Success(Some(Dummy("newValue"))))
    }

    "upsert the dummy in ZK and get it" in {
      dao.upsert("test1", Dummy("newValue"))
      dao.get("test1") should be(Success(Some(Dummy("newValue"))))
      dao.upsert("test1", Dummy("newValue2"))
      dao.get("test1") should be(Success(Some(Dummy("newValue2"))))
    }

    "delete the dummy in ZK and get it with a None result" in {
      dao.delete("test1")
      dao.exists("test1") should be (Success(false))
    }

    "save a dummy in ZK and delete all" in {
      dao.create("test1", Dummy("value"))
      dao.get("test1") should be(Success(Some(Dummy("value"))))
      dao.deleteAll
      dao.exists("test1") should be (Success(false))
      dao.count() shouldBe a[Failure[_]]
    }*/
  }

  "A Transaction manager backed by Zookeeper" should {

    import com.stratio.common.utils.MultiJVMTestUtils._
    import scala.language.postfixOps

    def sequenceGroups(outputs: Seq[String]): Seq[Seq[Int]] = {
      outputs.map(OutputEntry(_)).zipWithIndex groupBy {
        case (OutputEntry(label, _, _, _), n) => label
      } values
    }.toSeq.map(_.map(_._2))

    /*"avoid exclusion zone (over a given resource) process interleaving" in {

      val testBatch = (TestBatch() /: (1 to 5)) { (batchToUpdate, n) =>
        val p = externalProcess(ZKTransactionTestClient)(s"testclient$n", "1", "a", "5", (200 + n*10).toString)
        batchToUpdate addProcess p
      }

      sequenceGroups(testBatch.launchAndWait().view) foreach { group =>
        group should contain theSameElementsInOrderAs (group.min until (group.min + group.size))
      }

    }*/

    /*"avoid exclusion zone (over overlapping resources sets) process inverleaving" in {

      val testBatch = TestBatch() addProcess {
        externalProcess(ZKTransactionTestClient)("testclient1", "3", "a", "b", "c", "10", "300")
      } addProcess {
        externalProcess(ZKTransactionTestClient)("testclient2", "2", "c", "d", "10", "100")
      } addProcess {
        externalProcess(ZKTransactionTestClient)("testclient3", "2", "b", "c", "10", "150")
      }

      sequenceGroups(testBatch.launchAndWait().view) foreach { group =>
        group should contain theSameElementsInOrderAs (group.min until (group.min + group.size))
      }

    }*/

    /*"avoid process interleaving for intersecting resources while allowing interleaving for non-intersecting" in {

      val testBatch = TestBatch() addProcess {
        externalProcess(ZKTransactionTestClient)("testclient1", "2", "a", "b", "10", "100")
      } addProcess {
        externalProcess(ZKTransactionTestClient)("testclient2", "2", "b", "c", "10", "100")
      } addProcess {
        externalProcess(ZKTransactionTestClient)("testclient3", "2", "c", "d", "10", "100")
      }

      val output = testBatch.launchAndWait()

      val outOneAndTwo = output.filterNot(line => OutputEntry(line).client == "testclient3")
      val outTwoAndThree = output.filterNot(line => OutputEntry(line).client == "testclient1")
      val outOneAndThree = output.filterNot(line => OutputEntry(line).client == "testclient2")

      sequenceGroups(outOneAndTwo) foreach { group =>
        group should contain theSameElementsInOrderAs (group.min until (group.min + group.size))
      }

      sequenceGroups(outTwoAndThree) foreach { group =>
        group should contain theSameElementsInOrderAs (group.min until (group.min + group.size))
      }
      
    }*/


  }

}

class DummyZookeeperDAOComponent extends GenericDAOComponent[Dummy]
  with ZookeeperRepositoryComponent
  with TypesafeConfigComponent
  with Slf4jLoggerComponent {

  override val dao : DAO = new GenericDAO(Option("dummy"))

}

case class Dummy(property: String) {}
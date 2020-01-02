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

package com.stratio.common.utils.components.repository.impl

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.stratio.common.utils.components.config.ConfigComponent
import com.stratio.common.utils.components.logger.LoggerComponent
import com.stratio.common.utils.components.repository.RepositoryComponent
import org.apache.curator.framework.recipes.cache._
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.utils.CloseableUtils
import org.json4s.Formats
import org.json4s.jackson.Serialization.read

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}
import com.stratio.common.utils.components.repository.impl.ZookeeperRepositoryComponent._
import com.stratio.common.utils.functional.TryUtils


trait ZookeeperRepositoryComponent extends RepositoryComponent[String, Array[Byte]] {
  self: ConfigComponent with LoggerComponent =>

  val repository = new ZookeeperRepository()

  class ZookeeperRepository(path: Option[String] = None) extends Repository {

    protected def curatorClient: CuratorFramework =
      ZookeeperRepository.getInstance(getZookeeperConfig)

    def get(entity: String, id: String): Try[Option[Array[Byte]]] =
      Try(Option(curatorClient
        .getData
        .forPath(s"/$entity/$id")))

    def getAll(entity: String): Try[Seq[Array[Byte]]] =
      Try(curatorClient
        .getChildren
        .forPath(s"/$entity")).flatMap(entityIds =>
        TryUtils.sequence(entityIds.map(get(entity, _).map(_.get)))
      )

    def getNodes(entity: String): Try[Seq[String]] =
      Try(curatorClient
        .getChildren
        .forPath(s"/$entity"))

    def count(entity: String): Try[Long] =
      Try(curatorClient
        .getChildren
        .forPath(s"/$entity").size.toLong)


    override def existsPath(entity: String): Try[Boolean] =
      Try(Option(curatorClient
        .checkExists()
        .forPath(s"/$entity"))
      ).map(_.isDefined)

    def exists(entity: String, id: String): Try[Boolean] =
      existsPath(s"$entity/$id")

    def create(entity: String, id: String, element: Array[Byte]): Try[Array[Byte]] =
      Try(
        curatorClient
          .create()
          .creatingParentsIfNeeded()
          .forPath(s"/$entity/$id", element)
      ).flatMap( _ => get(entity, id).map(_.get))


    def upsert(entity: String, id: String, element: Array[Byte]): Try[Array[Byte]] =
      exists(entity, id).flatMap {
        case false => create(entity, id, element)
        case true => update(entity, id, element).flatMap(_ => get(entity, id).map(_.get))
      }

    def update(entity: String, id: String, element: Array[Byte]): Try[Unit] =
      Try(curatorClient
        .setData()
        .forPath(s"/$entity/$id", element)
      )

    def delete(entity: String, id: String): Try[Unit] =
      Try(curatorClient
        .delete()
        .forPath(s"/$entity/$id")
      )

    def deleteAll(entity: String): Try[Unit] =
      Try(curatorClient
        .delete().deletingChildrenIfNeeded()
        .forPath(s"/$entity")
      )

    def getZookeeperConfig: Config =
      config.getConfig(ConfigZookeeper)
        .getOrElse(throw ZookeeperRepositoryException(s"Zookeeper config not found"))

    def getConfig: Map[String, Any] =
      getZookeeperConfig.toMap

    def start: Boolean = Try(curatorClient.start()).isSuccess

    def stop: Boolean = ZookeeperRepository.stop(config)

    def addListener[T <: Serializable](entity: String,
                                       id: String, callback: (T, NodeCache) => Unit)
                                      (implicit jsonFormat: Formats, ev: Manifest[T]): Unit = {

      val nodeCache: NodeCache = new NodeCache(curatorClient, s"/$entity/$id")

      nodeCache.getListenable.addListener(
        new NodeCacheListener {
          override def nodeChanged(): Unit =
            Try(new String(nodeCache.getCurrentData.getData)) match {
              case Success(value) => callback(read[T](value), nodeCache)
              case Failure(e) => logger.error(s"NodeCache value: ${nodeCache.getCurrentData}", e)
            }
        }
      )
      nodeCache.start()
    }

    def addEntityListener(entity: String, callback: (PathChildrenCache) => Unit): Unit = {

      val nodeCache: PathChildrenCache = new PathChildrenCache(curatorClient, s"/$entity", true)

      nodeCache.getListenable.addListener(
        new PathChildrenCacheListener {
          override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit =
            callback(nodeCache)
        }
      )
      nodeCache.start()
    }
  }

  private[this] object ZookeeperRepository {

    /**
      * Aimed to serve its monitor lock. It is safer to use a private instance monitor lock rather
      * than this's. It prevents dead-locks derived from eternal invocations to `synchronize`
      */
    private object lockObject

    def partitionHandle(curator: CuratorFramework): Boolean = {
      val ans = curator.getZookeeperClient.isConnected &&
        curator.getZookeeperClient.getZooKeeper.getState.isConnected &&
        curator.getZookeeperClient.getZooKeeper.getState.isAlive
      val str = ans.toString
      println(s"zookeeper repository component -> getInstance: $str")
      ans
    }


    def getInstance(config: Config): CuratorFramework = lockObject.synchronized {
      val connectionString = config.getString(ZookeeperConnection, DefaultZookeeperConnection)
      logger.debug(s"Getting Curator Framework Instance for Connection String [$connectionString]")
      Option(
        CuratorFactoryMap.curatorFrameworks.get(connectionString).partition (partitionHandle)
      ).flatMap {
        case (connectedFramework, disconnectedFramework) =>
          // Assure disconnected framework is closed, if exists
          disconnectedFramework.foreach { f =>
            logger.debug(s"Closing disconnected Curator Framework for Connection String [$connectionString]")
            CloseableUtils.closeQuietly(f)
          }
          // Use connected framework, if exists
          connectedFramework.headOption
      }.getOrElse {
        Try {
          val client = CuratorFrameworkFactory.builder()
            .connectString(connectionString)
            .connectionTimeoutMs(config.getInt(ZookeeperConnectionTimeout, DefaultZookeeperConnectionTimeout))
            .sessionTimeoutMs(config.getInt(ZookeeperSessionTimeout, DefaultZookeeperSessionTimeout))
            .retryPolicy(
              new ExponentialBackoffRetry(
                config.getInt(ZookeeperRetryInterval, DefaultZookeeperRetryInterval),
                config.getInt(ZookeeperRetryAttemps, DefaultZookeeperRetryAttemps)))
            .build()
          logger.debug(s"Starting Curator Framework for Connection String [$connectionString]")
          client.start()
          client.blockUntilConnected(ZookeeperConnectionBlockUntilConnectedIntervalInSeconds, TimeUnit.SECONDS)
          logger.debug(s"Curator Framework for Connection String [$connectionString] connected")
          client
        } match {
          case Success(client: CuratorFramework) =>
            CuratorFactoryMap.curatorFrameworks.put(connectionString, client)
            client
          case Failure(_: Throwable) =>
            throw ZookeeperRepositoryException("Error trying to create a new Zookeeper instance")
        }
      }
    }

    def stopAll: Boolean = lockObject.synchronized {
      logger.debug(s"Stopping all Curator Framework Instances")
      Try {
        CuratorFactoryMap.curatorFrameworks.foreach {
          case (connectionString, curator) =>
            logger.debug(s"Curator Framework Instance for Connection String [$connectionString] stopped")
            CloseableUtils.closeQuietly(curator)
        }
        CuratorFactoryMap.curatorFrameworks.clear()
      }.isSuccess
    }

    def stop(config: Config): Boolean = lockObject.synchronized {
      val connectionString = config.getString(ZookeeperConnection, DefaultZookeeperConnection)
      Try {
        CuratorFactoryMap.curatorFrameworks.remove(connectionString).foreach(CloseableUtils.closeQuietly)
        logger.debug(s"Curator Framework Instance for Connection String [$connectionString] stopped")
      }.isSuccess
    }
  }

}

private[this] object CuratorFactoryMap {

  val curatorFrameworks: scala.collection.concurrent.Map[String, CuratorFramework] =
    new ConcurrentHashMap[String, CuratorFramework]()

}

object ZookeeperRepositoryComponent {

  val ZookeeperConnection = "connectionString"
  val DefaultZookeeperConnection = "localhost:2181"
  val ZookeeperConnectionTimeout = "connectionTimeout"
  val DefaultZookeeperConnectionTimeout = 15000
  val ZookeeperSessionTimeout = "sessionTimeout"
  val DefaultZookeeperSessionTimeout = 60000
  val ZookeeperRetryAttemps = "retryAttempts"
  val DefaultZookeeperRetryAttemps = 5
  val ZookeeperRetryInterval = "retryInterval"
  val DefaultZookeeperRetryInterval = 10000
  val ConfigZookeeper = "zookeeper"
  val ZookeeperConnectionBlockUntilConnectedIntervalInSeconds = 15
}

case class ZookeeperRepositoryException(msg: String) extends Exception(msg)

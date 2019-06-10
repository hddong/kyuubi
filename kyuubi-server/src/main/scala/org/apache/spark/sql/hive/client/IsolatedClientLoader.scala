/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.client

import java.lang.reflect.InvocationTargetException
import java.net.{URL, URLClassLoader}
import java.util

import org.apache.commons.io.IOUtils

import scala.util.Try
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.{KyuubiSparkUtil, SparkConf}
import org.apache.spark.sql.internal.NonClosableMutableURLClassLoader
import org.apache.spark.util.MutableURLClassLoader
import yaooqinn.kyuubi.Logging

/**
 * A Hacking Class for [[IsolatedClientLoader]] to be not isolated
 */
private[hive] object IsolatedClientLoader {
  def hiveVersion(version: String): HiveVersion = version match {
    case "12" | "0.12" | "0.12.0" => hive.v12
    case "13" | "0.13" | "0.13.0" | "0.13.1" => hive.v13
    case "14" | "0.14" | "0.14.0" => hive.v14
    case "1.0" | "1.0.0" => hive.v1_0
    case "1.1" | "1.1.0" => hive.v1_1
    case "1.2" | "1.2.0" | "1.2.1" | "1.2.2" => hive.v1_2
    case "2.0" | "2.0.0" | "2.0.1" => hive.v2_0
    case "2.1" | "2.1.0" | "2.1.1" => hive.v2_1
    case "3.0" | "3.0.0" => hive.v3_0
  }
}

private[hive] class IsolatedClientLoader(
                                          val version: HiveVersion,
                                          val sparkConf: SparkConf,
                                          val hadoopConf: Configuration,
                                          val execJars: Seq[URL] = Seq.empty,
                                          val config: Map[String, String] = Map.empty,
                                          val isolationOn: Boolean = true,
                                          val sharesHadoopClasses: Boolean = true,
                                          val rootClassLoader: ClassLoader = ClassLoader.getSystemClassLoader.getParent.getParent,
                                          val baseClassLoader: ClassLoader = Thread.currentThread().getContextClassLoader,
                                          val sharedPrefixes: Seq[String] = Seq.empty,
                                          val barrierPrefixes: Seq[String] = Seq.empty)
  extends Logging {

  import KyuubiSparkUtil._

  // Check to make sure that the root classloader does not know about Hive.
  assert(Try(rootClassLoader.loadClass("org.apache.hadoop.hive.conf.HiveConf")).isFailure)

  /** All jars used by the hive specific classloader. */
  protected def allJars = execJars.toArray

  protected def isSharedClass(name: String): Boolean = {
    val isHadoopClass =
      name.startsWith("org.apache.hadoop.") && !name.startsWith("org.apache.hadoop.hive.")

    name.contains("slf4j") ||
      name.contains("log4j") ||
      name.startsWith("org.apache.spark.") ||
      (sharesHadoopClasses && isHadoopClass) ||
      name.startsWith("scala.") ||
      (name.startsWith("com.google") && !name.startsWith("com.google.cloud")) ||
      name.startsWith("java.lang.") ||
      name.startsWith("java.net") ||
      sharedPrefixes.exists(name.startsWith)
  }

  /** True if `name` refers to a spark class that must see specific version of Hive. */
  protected def isBarrierClass(name: String): Boolean =
    name.startsWith(classOf[HiveClientImpl].getName) ||
      name.startsWith(classOf[Shim].getName) ||
      barrierPrefixes.exists(name.startsWith)

  protected def classToPath(name: String): String =
    name.replaceAll("\\.", "/") + ".class"

  /**
   * (Kent Yao) Different with Spark internal which use an isolated classloader to support different
   * Hive versions, Kyuubi believe that the hive 1.2.1 is capable to support 1.2 or higher version
   * Hive metastore servers and the elder hive client versions are not worth to support.
   *
   * ANOTHER reason here we close the isolation is because Spark don't expose authorization
   * functions in [[HiveClient]], which is unable to invoke these methods in different classloaders
   *
   * Besides, [[HiveClient]] in normal Spark applications is globally one instance, so this
   * classloader could/should be non-closeable. But in Kyuubi, this is a session level object
   * associated with one KyuubiSession/SparkSession, thus, this classloader should be closeable to
   * support class unloading.
   *
   * This classloader is a special URLClassLoader that exposes the addURL method.
   * So, when we add jar, we can add this new jar directly through the addURL method
   * instead of stacking a new URLClassLoader on top of it.
   */
  /**
   * The classloader that is used to load an isolated version of Hive.
   * This classloader is a special URLClassLoader that exposes the addURL method.
   * So, when we add jar, we can add this new jar directly through the addURL method
   * instead of stacking a new URLClassLoader on top of it.
   */
  private[hive] val classLoader: MutableURLClassLoader = {
    val isolatedClassLoader =
      if (isolationOn) {
        new URLClassLoader(allJars, rootClassLoader) {
          override def loadClass(name: String, resolve: Boolean): Class[_] = {
            val loaded = findLoadedClass(name)
            if (loaded == null) doLoadClass(name, resolve) else loaded
          }
          def doLoadClass(name: String, resolve: Boolean): Class[_] = {
            val classFileName = name.replaceAll("\\.", "/") + ".class"
            if (isBarrierClass(name)) {
              // For barrier classes, we construct a new copy of the class.
              val bytes = IOUtils.toByteArray(baseClassLoader.getResourceAsStream(classFileName))
              info(s"custom defining: $name - ${util.Arrays.hashCode(bytes)}")
              defineClass(name, bytes, 0, bytes.length)
            } else if (!isSharedClass(name)) {
              info(s"hive class: $name - ${getResource(classToPath(name))}")
              super.loadClass(name, resolve)
            } else {
              // For shared classes, we delegate to baseClassLoader, but fall back in case the
              // class is not found.
              info(s"shared class: $name")
              try {
                baseClassLoader.loadClass(name)
              } catch {
                case _: ClassNotFoundException =>
                  super.loadClass(name, resolve)
              }
            }
          }
        }
      } else {
        baseClassLoader
      }
    // Right now, we create a URLClassLoader that gives preference to isolatedClassLoader
    // over its own URLs when it loads classes and resources.
    // We may want to use ChildFirstURLClassLoader based on
    // the configuration of spark.executor.userClassPathFirst, which gives preference
    // to its own URLs over the parent class loader (see Executor's createClassLoader method).
    new NonClosableMutableURLClassLoader(isolatedClassLoader)
//    new MutableURLClassLoader(Array.empty, baseClassLoader)
  }

  private[hive] def addJar(path: URL): Unit = {
    classLoader.addURL(path)
  }

  /** The isolated client interface to Hive. */
  private[hive] def createClient(): HiveClient = synchronized {
    if (!isolationOn) {
      return new HiveClientImpl(version, sparkConf, hadoopConf, config, baseClassLoader, this)
    }
    // Pre-reflective instantiation setup.
    val origLoader = Thread.currentThread().getContextClassLoader
    Thread.currentThread.setContextClassLoader(classLoader)

    try {
      classLoader
        .loadClass(classOf[HiveClientImpl].getName)
        .getConstructors.head
        .newInstance(version, sparkConf, hadoopConf, config, classLoader, this)
        .asInstanceOf[HiveClient]
    } catch {
      case e: InvocationTargetException =>
        if (e.getCause().isInstanceOf[NoClassDefFoundError]) {
          val cnf = e.getCause().asInstanceOf[NoClassDefFoundError]
          throw new ClassNotFoundException(
            s"$cnf when creating Hive client using classpath: ${execJars.mkString(", ")}\n" +
              "Please make sure that jars for your version of hive and hadoop are included in the " +
              s"paths passed to ${HiveUtils.HIVE_METASTORE_JARS.key}.", e)
        } else {
          throw e
        }
    } finally {
      Thread.currentThread.setContextClassLoader(origLoader)
    }
  }
//
//  /** The isolated client interface to Hive. */
//  private[hive] def createClient(): HiveClient = synchronized {
//
//    val ctor = classOf[HiveClientImpl].getConstructors.head
//    if (majorVersion(SPARK_VERSION) == 2 && minorVersion(SPARK_VERSION) > 3) {
//      val warehouseDir = Option(hadoopConf.get(ConfVars.METASTOREWAREHOUSE.varname))
//      ctor.newInstance(
//        version,
//        warehouseDir,
//        sparkConf,
//        hadoopConf,
//        config,
//        classLoader,
//        this).asInstanceOf[HiveClientImpl]
//    } else {
//      ctor.newInstance(
//        version,
//        sparkConf,
//        hadoopConf,
//        config,
//        classLoader,
//        this).asInstanceOf[HiveClientImpl]
//    }
//
//  }

  /**
   * The place holder for shared Hive client for all the HiveContext sessions (they share an
   * IsolatedClientLoader).
   */
  private[hive] var cachedHive: Any = null
}
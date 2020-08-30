package org.apache.spark.sql.execution.datasources.clickhouse

import java.sql.{Driver, DriverManager}

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * java.sql.DriverManager is always loaded by bootstrap classloader,
 * so it can't load JDBC drivers accessible by Spark ClassLoader.
 *
 * To solve the problem, drivers from user-supplied jars are wrapped into thin wrapper.
 */
object DriverRegistry extends Logging {

  /**
   * Load DriverManager first to avoid any race condition between
   * DriverManager static initialization block and specific driver class's
   * static initialization block. e.g. PhoenixDriver
   */
  DriverManager.getDrivers

  private val wrapperMap: mutable.Map[String, DriverWrapper] = mutable.Map.empty

  def register(className: String): Unit = {
    val cls = Utils.getContextOrSparkClassLoader.loadClass(className)
    if (cls.getClassLoader == null) {
      logTrace(s"$className has been loaded with bootstrap ClassLoader, wrapper is not required")
    } else if (wrapperMap.get(className).isDefined) {
      logTrace(s"Wrapper for $className already exists")
    } else {
      synchronized {
        if (wrapperMap.get(className).isEmpty) {
          val wrapper = new DriverWrapper(cls.newInstance().asInstanceOf[Driver])
          DriverManager.registerDriver(wrapper)
          wrapperMap(className) = wrapper
          logTrace(s"Wrapper for $className registered")
        }
      }
    }
  }
}


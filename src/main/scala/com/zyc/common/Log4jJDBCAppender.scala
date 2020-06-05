package com.zyc.common

import java.sql.{Connection, DriverManager}

import org.apache.log4j.jdbc.JDBCAppender

class Log4jJDBCAppender extends JDBCAppender{

  override def getConnection: Connection = {

    if (!DriverManager.getDrivers().hasMoreElements())
      setDriver("com.mysql.cj.jdbc.Driver")

    if ((null != connection) && !connection.isValid(5000)) {
      println("日志服务器重连......")
      connection = null;
    }

    if (connection == null) {
      connection = DriverManager.getConnection(databaseURL, databaseUser, databasePassword);
    }

    return connection;


  }

}

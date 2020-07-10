package org.apache.spark.sql.hive_jdbc.datasources.clickhouse

import java.sql.{Connection, PreparedStatement, ResultSet}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CompletionIterator

import scala.util.control.NonFatal


object ClickHouseRDD{


  def resolveTable(options: HiveOptions): StructType = {
    val url = options.url
    val table = options.tableOrQuery
    val dialect = ClickHouseDialect
    val conn: Connection = ClickHouseUtils.createConnectionFactory(options)()
    try {
      val statement = conn.prepareStatement(dialect.getSchemaQuery(table))
      try {
        //statement.setQueryTimeout(options.queryTimeout)
        val rs = statement.executeQuery()
        try {
          ClickHouseUtils.getSchema(rs, dialect, alwaysNullable = true)
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

  /**
    * Prune all but the specified columns from the specified Catalyst schema.
    *
    * @param schema - The Catalyst schema of the master table
    * @param columns - The list of desired columns
    *
    * @return A Catalyst schema corresponding to columns in the given order.
    */
  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }

  def compileFilter(f: Filter, dialect: JdbcDialect): Option[String] = {
    def quote(colName: String): String = dialect.quoteIdentifier(colName)

    Option(f match {
      case EqualTo(attr, value) => s"${quote(attr)} = ${dialect.compileValue(value)}"
      case EqualNullSafe(attr, value) =>
        val col = quote(attr)
        s"(NOT ($col != ${dialect.compileValue(value)} OR $col IS NULL OR " +
          s"${dialect.compileValue(value)} IS NULL) OR " +
          s"($col IS NULL AND ${dialect.compileValue(value)} IS NULL))"
      case LessThan(attr, value) => s"${quote(attr)} < ${dialect.compileValue(value)}"
      case GreaterThan(attr, value) => s"${quote(attr)} > ${dialect.compileValue(value)}"
      case LessThanOrEqual(attr, value) => s"${quote(attr)} <= ${dialect.compileValue(value)}"
      case GreaterThanOrEqual(attr, value) => s"${quote(attr)} >= ${dialect.compileValue(value)}"
      case IsNull(attr) => s"${quote(attr)} IS NULL"
      case IsNotNull(attr) => s"${quote(attr)} IS NOT NULL"
      case StringStartsWith(attr, value) => s"${quote(attr)} LIKE '${value}%'"
      case StringEndsWith(attr, value) => s"${quote(attr)} LIKE '%${value}'"
      case StringContains(attr, value) => s"${quote(attr)} LIKE '%${value}%'"
      case In(attr, value) if value.isEmpty =>
        s"CASE WHEN ${quote(attr)} IS NULL THEN NULL ELSE FALSE END"
      case In(attr, value) => s"${quote(attr)} IN (${dialect.compileValue(value)})"
      case Not(f) => compileFilter(f, dialect).map(p => s"(NOT ($p))").getOrElse(null)
      case Or(f1, f2) =>
        // We can't compile Or filter unless both sub-filters are compiled successfully.
        // It applies too for the following And filter.
        // If we can make sure compileFilter supports all filters, we can remove this check.
        val or = Seq(f1, f2).flatMap(compileFilter(_, dialect))
        if (or.size == 2) {
          or.map(p => s"($p)").mkString(" OR ")
        } else {
          null
        }
      case And(f1, f2) =>
        val and = Seq(f1, f2).flatMap(compileFilter(_, dialect))
        if (and.size == 2) {
          and.map(p => s"($p)").mkString(" AND ")
        } else {
          null
        }
      case _ => null
    })
  }

  def scanTable(
                 sc: SparkContext,
                 schema: StructType,
                 requiredColumns: Array[String],
                 filters: Array[Filter],
                 parts: Array[Partition],
                 options: HiveOptions): RDD[InternalRow] = {
    val url = options.url
    val dialect =ClickHouseDialect
    val quotedColumns = requiredColumns.map(colName => dialect.quoteIdentifier(colName))
    new ClickHouseRDD(
      sc,
      ClickHouseUtils.createConnectionFactory(options),
      pruneSchema(schema, requiredColumns),
      quotedColumns,
      filters,
      parts,
      url,
      options)
  }

}


class ClickHouseRDD(
               sc: SparkContext,
               getConnection: () => Connection,
               schema: StructType,
               columns: Array[String],
               filters: Array[Filter],
               partitions: Array[Partition],
               url: String,
               options: HiveOptions)
  extends RDD[InternalRow](sc, Nil) {

  /**
    * Retrieve the list of partitions corresponding to this RDD.
    */
  override def getPartitions: Array[Partition] = partitions

  /**
    * `columns`, but as a String suitable for injection into a SQL query.
    */
  private val columnList: String = {
    val sb = new StringBuilder()
    columns.foreach(x => sb.append(",").append(x))
    if (sb.isEmpty) "1" else sb.substring(1)
  }

  /**
    * `filters`, but as a WHERE clause suitable for injection into a SQL query.
    */
  private val filterWhereClause: String =
    filters
      .flatMap(ClickHouseRDD.compileFilter(_, ClickHouseDialect))
      .map(p => s"($p)").mkString(" AND ")

  /**
    * A WHERE clause representing both `filters`, if any, and the current partition.
    */
  private def getWhereClause(part: HivePartition): String = {
    if (part.whereClause != null && filterWhereClause.length > 0) {
      "WHERE " + s"($filterWhereClause)" + " AND " + s"(${part.whereClause})"
    } else if (part.whereClause != null) {
      "WHERE " + part.whereClause
    } else if (filterWhereClause.length > 0) {
      "WHERE " + filterWhereClause
    } else {
      ""
    }
  }

  /**
    * Runs the SQL query against the JDBC driver.
    *
    */
  override def compute(thePart: Partition, context: TaskContext): Iterator[InternalRow] = {
    var closed = false
    var rs: ResultSet = null
    var stmt: PreparedStatement = null
    var conn: Connection = null

    def close() {
      if (closed) return
      try {
        if (null != rs) {
          rs.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing resultset", e)
      }
      try {
        if (null != stmt) {
          stmt.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      try {
        if (null != conn) {
          if (!conn.isClosed && !conn.getAutoCommit) {
            try {
              conn.commit()
            } catch {
              case NonFatal(e) => logWarning("Exception committing transaction", e)
            }
          }
          conn.close()
        }
        logInfo("closed connection")
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
      closed = true
    }

    context.addTaskCompletionListener[Unit]{ context => close() }

    val inputMetrics = context.taskMetrics().inputMetrics
    val part = thePart.asInstanceOf[HivePartition]
    conn = getConnection()
    val dialect = ClickHouseDialect
    import scala.collection.JavaConverters._
    dialect.beforeFetch(conn, options.asProperties.asScala.toMap)

    // This executes a generic SQL statement (or PL/SQL block) before reading
    // the table/query via JDBC. Use this feature to initialize the database
    // session environment, e.g. for optimizations and/or troubleshooting.
    options.sessionInitStatement match {
      case Some(sql) =>
        val statement = conn.prepareStatement(sql)
        logInfo(s"Executing sessionInitStatement: $sql")
        try {
          statement.setQueryTimeout(options.queryTimeout)
          statement.execute()
        } finally {
          statement.close()
        }
      case None =>
    }

    // H2's JDBC driver does not support the setSchema() method.  We pass a
    // fully-qualified table name in the SELECT statement.  I don't know how to
    // talk about a table in a completely portable way.

    val myWhereClause = getWhereClause(part)

    val sqlText = s"SELECT $columnList FROM ${options.tableOrQuery} $myWhereClause"
    stmt = conn.prepareStatement(sqlText,
      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
  //  stmt.setFetchSize(options.fetchSize)
  //  stmt.setQueryTimeout(options.queryTimeout)
    rs = stmt.executeQuery()

//    val rsmd = rs.getMetaData
//    val ncols = rsmd.getColumnCount
//    //val fields = new Array[StructField](ncols)
//    var rs_req=Seq.empty[InternalRow]
//
//
//    while (rs.next()){
//      val seq = new Array[Any](ncols)
//      var i = 0
//      while (i < ncols) {
//        val columnName = rsmd.getColumnLabel(i + 1)
//        val dataType = rsmd.getColumnType(i + 1)
//        val typeName = rsmd.getColumnTypeName(i + 1)
//        val fieldSize = rsmd.getPrecision(i + 1)
//        val fieldScale = rsmd.getScale(i + 1)
//        seq(i)=rs.getString(i+1)
//        i=i+1
//      }
//
//      rs_req=rs_req.:+(InternalRow.fromSeq(seq))
//    }
//    rs_req.foreach(f=>println(f.toString))

  //  stmt.close()
   // rs.close()

    val rowsIterator = ClickHouseUtils.resultSetToSparkInternalRows(rs, schema, inputMetrics)
//    rowsIterator.map(encoder.fromRow(_))
//    println(rowsIterator.size)
//
    CompletionIterator[InternalRow, Iterator[InternalRow]](
      new InterruptibleIterator(context, rowsIterator), close())

  }
}


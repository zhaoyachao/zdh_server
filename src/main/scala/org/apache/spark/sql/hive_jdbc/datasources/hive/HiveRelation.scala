package org.apache.spark.sql.hive_jdbc.datasources.hive

import java.sql.{Date, Timestamp}

import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer

case class HivePartition(whereClause: String, idx: Int) extends Partition {
  override def index: Int = idx
}

private[sql] case class HivePartitioningInfo(
                                              column: String,
                                              columnType: DataType,
                                              lowerBound: Long,
                                              upperBound: Long,
                                              numPartitions: Int)


private[sql] object HiveRelation extends Logging {
  /**
    * Given a partitioning schematic (a column of integral type, a number of
    * partitions, and upper and lower bounds on the column's value), generate
    * WHERE clauses for each partition so that each row in the table appears
    * exactly once.  The parameters minValue and maxValue are advisory in that
    * incorrect values may cause the partitioning to be poor, but no data
    * will fail to be represented.
    *
    * Null value predicate is added to the first partition where clause to include
    * the rows with null value for the partitions column.
    *
    * @param schema      resolved schema of a JDBC table
    * @param resolver    function used to determine if two identifiers are equal
    * @param timeZoneId  timezone ID to be used if a partition column type is date or timestamp
    * @param jdbcOptions JDBC options that contains url
    * @return an array of partitions with where clause for each partition
    */
  def columnPartition(
                       schema: StructType,
                       resolver: Resolver,
                       timeZoneId: String,
                       jdbcOptions: HiveOptions): Array[Partition] = {
    val partitioning = {
      import HiveOptions._

      val partitionColumn = jdbcOptions.partitionColumn
      val lowerBound = jdbcOptions.lowerBound
      val upperBound = jdbcOptions.upperBound
      val numPartitions = jdbcOptions.numPartitions

      if (partitionColumn.isEmpty) {
        assert(lowerBound.isEmpty && upperBound.isEmpty, "When 'partitionColumn' is not " +
          s"specified, '$JDBC_LOWER_BOUND' and '$JDBC_UPPER_BOUND' are expected to be empty")
        null
      } else {
        assert(lowerBound.nonEmpty && upperBound.nonEmpty && numPartitions.nonEmpty,
          s"When 'partitionColumn' is specified, '$JDBC_LOWER_BOUND', '$JDBC_UPPER_BOUND', and " +
            s"'$JDBC_NUM_PARTITIONS' are also required")

        val (column, columnType) = verifyAndGetNormalizedPartitionColumn(
          schema, partitionColumn.get, resolver, jdbcOptions)

        val lowerBoundValue = toInternalBoundValue(lowerBound.get, columnType)
        val upperBoundValue = toInternalBoundValue(upperBound.get, columnType)
        HivePartitioningInfo(
          column, columnType, lowerBoundValue, upperBoundValue, numPartitions.get)
      }
    }

    if (partitioning == null || partitioning.numPartitions <= 1 ||
      partitioning.lowerBound == partitioning.upperBound) {
      return Array[Partition](HivePartition(null, 0))
    }

    val lowerBound = partitioning.lowerBound
    val upperBound = partitioning.upperBound
    require(lowerBound <= upperBound,
      "Operation not allowed: the lower bound of partitioning column is larger than the upper " +
        s"bound. Lower bound: $lowerBound; Upper bound: $upperBound")

    val boundValueToString: Long => String =
      toBoundValueInWhereClause(_, partitioning.columnType, timeZoneId)
    val numPartitions =
      if ((upperBound - lowerBound) >= partitioning.numPartitions || /* check for overflow */
        (upperBound - lowerBound) < 0) {
        partitioning.numPartitions
      } else {
        logWarning("The number of partitions is reduced because the specified number of " +
          "partitions is less than the difference between upper bound and lower bound. " +
          s"Updated number of partitions: ${upperBound - lowerBound}; Input number of " +
          s"partitions: ${partitioning.numPartitions}; " +
          s"Lower bound: ${boundValueToString(lowerBound)}; " +
          s"Upper bound: ${boundValueToString(upperBound)}.")
        upperBound - lowerBound
      }
    // Overflow and silliness can happen if you subtract then divide.
    // Here we get a little roundoff, but that's (hopefully) OK.
    val stride: Long = upperBound / numPartitions - lowerBound / numPartitions

    var i: Int = 0
    val column = partitioning.column
    var currentValue = lowerBound
    val ans = new ArrayBuffer[Partition]()
    while (i < numPartitions) {
      val lBoundValue = boundValueToString(currentValue)
      val lBound = if (i != 0) s"$column >= $lBoundValue" else null
      currentValue += stride
      val uBoundValue = boundValueToString(currentValue)
      val uBound = if (i != numPartitions - 1) s"$column < $uBoundValue" else null
      val whereClause =
        if (uBound == null) {
          lBound
        } else if (lBound == null) {
          s"$uBound or $column is null"
        } else {
          s"$lBound AND $uBound"
        }
      ans += HivePartition(whereClause, i)
      i = i + 1
    }
    val partitions = ans.toArray
    logInfo(s"Number of partitions: $numPartitions, WHERE clauses of these partitions: " +
      partitions.map(_.asInstanceOf[HivePartition].whereClause).mkString(", "))
    partitions
  }

  // Verify column name and type based on the JDBC resolved schema
  private def verifyAndGetNormalizedPartitionColumn(
                                                     schema: StructType,
                                                     columnName: String,
                                                     resolver: Resolver,
                                                     jdbcOptions: HiveOptions): (String, DataType) = {
    val dialect = HiveDialect
    val column = schema.find { f =>
      resolver(f.name, columnName) || resolver(dialect.quoteIdentifier(f.name), columnName)
    }.getOrElse {
      throw new AnalysisException(s"User-defined partition column $columnName not " +
        s"found in the JDBC relation: ${schema.simpleString(Utils.maxNumToStringFields)}")
    }
    column.dataType match {
      case _: NumericType | DateType | TimestampType =>
      case _ =>
        throw new AnalysisException(
          s"Partition column type should be ${NumericType.simpleString}, " +
            s"${DateType.catalogString}, or ${TimestampType.catalogString}, but " +
            s"${column.dataType.catalogString} found.")
    }
    (dialect.quoteIdentifier(column.name), column.dataType)
  }

  private def toInternalBoundValue(value: String, columnType: DataType): Long = columnType match {
    case _: NumericType => value.toLong
    case DateType => DateTimeUtils.fromJavaDate(Date.valueOf(value)).toLong
    case TimestampType => DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(value))
  }

  private def toBoundValueInWhereClause(
                                         value: Long,
                                         columnType: DataType,
                                         timeZoneId: String): String = {
    def dateTimeToString(): String = {
      val timeZone = DateTimeUtils.getTimeZone(timeZoneId)
      val dateTimeStr = columnType match {
        case DateType => DateTimeUtils.dateToString(value.toInt, timeZone)
        case TimestampType => DateTimeUtils.timestampToString(value, timeZone)
      }
      s"'$dateTimeStr'"
    }

    columnType match {
      case _: NumericType => value.toString
      case DateType | TimestampType => dateTimeToString()
    }
  }

  /**
    * Takes a (schema, table) specification and returns the table's Catalyst schema.
    * If `customSchema` defined in the JDBC options, replaces the schema's dataType with the
    * custom schema's type.
    *
    * @param resolver    function used to determine if two identifiers are equal
    * @param jdbcOptions JDBC options that contains url, table and other information.
    * @return resolved Catalyst schema of a JDBC table
    */
  def getSchema(resolver: Resolver, jdbcOptions: HiveOptions): StructType = {
    val tableSchema = HiveRDD.resolveTable(jdbcOptions)
    jdbcOptions.customSchema match {
      case Some(customSchema) => HiveUtils.getCustomSchema(
        tableSchema, customSchema, resolver)
      case None => tableSchema
    }
  }

  /**
    * Resolves a Catalyst schema of a JDBC table and returns [[HiveRelation]] with the schema.
    */
  def apply(
             parts: Array[Partition],
             jdbcOptions: HiveOptions)(
             sparkSession: SparkSession): HiveRelation = {
    val schema = HiveRelation.getSchema(sparkSession.sessionState.conf.resolver, jdbcOptions)
    HiveRelation(schema, parts, jdbcOptions)(sparkSession)
  }
}


case class HiveRelation(
                         override val schema: StructType,
                         parts: Array[Partition],
                         jdbcOptions: HiveOptions)(@transient val sparkSession: SparkSession)
  extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation {
  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    val rowsIterator = HiveRDD.scanTable(
      sparkSession.sparkContext,
      schema,
      requiredColumns,
      filters,
      parts,
      jdbcOptions)


    val encoder = RowEncoder(schema).resolveAndBind()
    rowsIterator.map(encoder.fromRow(_))
      .asInstanceOf[RDD[Row]]
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {

    data.write
      .mode(if (overwrite) SaveMode.Overwrite else SaveMode.Append)
      .jdbc(jdbcOptions.url, jdbcOptions.tableOrQuery, jdbcOptions.asProperties)

  }
}

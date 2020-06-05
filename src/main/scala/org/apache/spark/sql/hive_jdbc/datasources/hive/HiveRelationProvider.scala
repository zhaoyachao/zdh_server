package org.apache.spark.sql.hive_jdbc.datasources.hive

import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive_jdbc.datasources.hive.HiveUtils._
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}

class HiveRelationProvider extends CreatableRelationProvider
  with RelationProvider with DataSourceRegister{
  override def shortName() = "hive_jdbc"


  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String],
                              df: DataFrame) = {
    val options = new HiveOptionsInWrite(parameters)
    val isCaseSensitive = sqlContext.conf.caseSensitiveAnalysis

    val conn = HiveUtils.createConnectionFactory(options)()
    try {
      val tableExists = HiveUtils.tableExists(conn, options)
      if (tableExists) {
        mode match {
          case SaveMode.Overwrite =>
            if (options.isTruncate && isCascadingTruncateTable(options.url) == Some(false)) {
              // In this case, we should truncate table and then load.
              truncateTable(conn, options)
              val tableSchema = HiveUtils.getSchemaOption(conn, options)
              saveTable(df, tableSchema, isCaseSensitive, options)
            } else {
              // Otherwise, do not truncate the table, instead drop and recreate it
              dropTable(conn, options.table, options)
              createTable(conn, df, options)
              saveTable(df, Some(df.schema), isCaseSensitive, options)
            }

          case SaveMode.Append =>
            val tableSchema = HiveUtils.getSchemaOption(conn, options)
            saveTable(df, tableSchema, isCaseSensitive, options)

          case SaveMode.ErrorIfExists =>
            throw new AnalysisException(
              s"Table or view '${options.table}' already exists. " +
                s"SaveMode: ErrorIfExists.")

          case SaveMode.Ignore =>
          // With `SaveMode.Ignore` mode, if table already exists, the save operation is expected
          // to not save the contents of the DataFrame and to not change the existing data.
          // Therefore, it is okay to do nothing here and then just return the relation below.
        }
      } else {
        createTable(conn, df, options)
        saveTable(df, Some(df.schema), isCaseSensitive, options)
      }
    } finally {
      conn.close()
    }

    createRelation(sqlContext, parameters)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]):BaseRelation = {

    import sqlContext.implicits._
    val hiveOptions = new HiveOptions(parameters)
    val resolver = sqlContext.conf.resolver
    val timeZoneId = sqlContext.conf.sessionLocalTimeZone
    val schema = HiveRelation.getSchema(resolver, hiveOptions)
    val parts = HiveRelation.columnPartition(schema, resolver, timeZoneId, hiveOptions)
    HiveRelation(schema, parts, hiveOptions)(sqlContext.sparkSession)

  }


}

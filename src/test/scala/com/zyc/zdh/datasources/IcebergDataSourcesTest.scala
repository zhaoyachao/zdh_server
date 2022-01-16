package com.zyc.zdh.datasources

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.types.Types
import org.junit.Test
import org.scalatest.FunSuite
@Test
class IcebergDataSourcesTest {

  @Test
  def testWriteDS {

    val config = new Configuration()
    val catalog = new HadoopCatalog(config,"/data/iceberg");
    val schema=new iceberg.Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "user_name", Types.StringType.get()),
      Types.NestedField.required(3, "user_password", Types.StringType.get()),
      Types.NestedField.required(4, "eamil", Types.StringType.get()),
      Types.NestedField.required(5, "is_use_email", Types.StringType.get()),
      Types.NestedField.required(6, "phone", Types.StringType.get()),
      Types.NestedField.required(7, "is_use_phone", Types.StringType.get())
    )
    var name: TableIdentifier = TableIdentifier.of(Namespace.of("test"),"account_info")
    if(!catalog.tableExists(name)){
      var spec:PartitionSpec=PartitionSpec.unpartitioned()

      if(!catalog.namespaceExists(Namespace.of("test"))){
        catalog.createNamespace(Namespace.of("test"))
      }
      catalog.createTable(name, schema, spec)
    }

  }

}

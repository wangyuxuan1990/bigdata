package com.wangyuxuan.flink.demo7

import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.flink.api.java.tuple
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, HConstants, TableName}

/**
 * @author wangyuxuan
 * @date 2020/3/20 11:13
 * @description Flink集成Hbase之数据读取
 */
object FlinkReadHBase {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val hbaseData: DataSet[tuple.Tuple2[String, String]] = environment.createInput(new TableInputFormat[tuple.Tuple2[String, String]] {
      // 初始化方法
      override def configure(parameters: Configuration): Unit = {
        val conf = HBaseConfiguration.create()
        conf.set(HConstants.ZOOKEEPER_QUORUM, "node01,node02,node03")
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
        val conn: Connection = ConnectionFactory.createConnection(conf)
        table = classOf[HTable].cast(conn.getTable(TableName.valueOf("hbasesource")))
        //        table = conn.getTable(TableName.valueOf("hbasesource")).asInstanceOf[HTable]
        scan = new Scan() {
          addFamily(Bytes.toBytes("f1"))
        }
      }

      // 获取scan条件
      override def getScanner: Scan = {
        scan
      }

      // 获取查询的表名
      override def getTableName: String = {
        "hbasesource"
      }

      // 使用mr的程序去读取数据，然后转换成为我们想要的元组
      override def mapResultToTuple(result: Result): tuple.Tuple2[String, String] = {
        val rowKey: String = Bytes.toString(result.getRow)
        val sb: StringBuffer = new StringBuffer()
        for (cell: Cell <- result.rawCells()) {
          val value: String = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
          sb.append(value).append(",")
        }
        val valueString: String = sb.replace(sb.length() - 1, sb.length(), "").toString
        val tuple2 = new org.apache.flink.api.java.tuple.Tuple2[String, String]()
        tuple2.setField(rowKey, 0)
        tuple2.setField(valueString, 1)
        tuple2
      }
    })
    hbaseData.print()
    environment.execute()
  }
}

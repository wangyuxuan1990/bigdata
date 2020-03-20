package com.wangyuxuan.flink.demo7

import java.util

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.conf
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}

/**
 * @author wangyuxuan
 * @date 2020/3/20 13:22
 * @description Flink集成Hbase之数据写入
 *              第一种：实现OutputFormat接口
 *              第二种：继承RichSinkFunction重写父类方法
 */
object FlinkWriteHBase {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val sourceDataSet: DataSet[String] = environment.fromElements("01,zhangsan,28", "02,lisi,30")
    sourceDataSet.output(new HBaseOutputFormat)
    environment.execute()
  }
}

class HBaseOutputFormat extends OutputFormat[String] {
  val zkServer = "node01,node02,node03"
  val port = "2181"
  var conn: Connection = null

  /**
   * 配置写在这里
   *
   * @param parameters
   */
  override def configure(parameters: Configuration): Unit = {
    val config: conf.Configuration = HBaseConfiguration.create()
    config.set(HConstants.ZOOKEEPER_QUORUM, zkServer)
    config.set(HConstants.ZOOKEEPER_CLIENT_PORT, port)
    config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
    config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)
    conn = ConnectionFactory.createConnection(config)
  }

  /**
   * 初始化方法
   *
   * @param taskNumber
   * @param numTasks
   */
  override def open(taskNumber: Int, numTasks: Int): Unit = {

  }

  override def writeRecord(record: String): Unit = {
    val tableName: TableName = TableName.valueOf("hbasesource")
    val cf1 = "f1"
    val array: Array[String] = record.split(",")
    val put: Put = new Put(Bytes.toBytes(array(0)))
    put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("name"), Bytes.toBytes(array(1)))
    put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("age"), Bytes.toBytes(array(2)))
    val putList: util.ArrayList[Put] = new util.ArrayList[Put]()
    putList.add(put)
    // 设置缓存1m，当达到1m时数据会自动刷到hbase
    val params: BufferedMutatorParams = new BufferedMutatorParams(tableName)
    // 设置缓存的大小
    params.writeBufferSize(1024 * 1024)
    val mutator: BufferedMutator = conn.getBufferedMutator(params)
    mutator.mutate(putList)
    mutator.flush()
    putList.clear()
  }

  override def close(): Unit = {
    if (null != conn) {
      conn.close()
    }
  }
}
package com.wangyuxuan.kafka.demo5

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, OffsetRange}
import org.apache.spark.streaming.scheduler._

/**
 * @author wangyuxuan
 * @date 2020/3/5 10:47
 * @description 监听器
 */
class KafkaListener(var stream: InputDStream[ConsumerRecord[String, String]]) extends StreamingListener {
  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = super.onStreamingStarted(streamingStarted)

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = super.onReceiverStarted(receiverStarted)

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = super.onReceiverError(receiverError)

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = super.onReceiverStopped(receiverStopped)

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    val info: Map[Int, StreamInputInfo] = batchSubmitted.batchInfo.streamIdToInputInfo

    var offsetRangesTmp: List[OffsetRange] = null
    var offsetRanges: Array[OffsetRange] = null

    for (k <- info) {
      val offset: Option[Any] = k._2.metadata.get("offsets")

      if (!offset.isEmpty) {
        try {
          val offsetValue = offset.get
          offsetRangesTmp = offsetValue.asInstanceOf[List[OffsetRange]]
          offsetRanges = offsetRangesTmp.toSet.toArray
        } catch {
          case e: Exception => println(e)
        }
      }
    }
    if (offsetRanges != null) {
      // 因为我看了官网，所以我就知道这样就可以提交偏移量
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {

  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {

  }

  override def onOutputOperationStarted(outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = super.onOutputOperationStarted(outputOperationStarted)

  override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = super.onOutputOperationCompleted(outputOperationCompleted)
}

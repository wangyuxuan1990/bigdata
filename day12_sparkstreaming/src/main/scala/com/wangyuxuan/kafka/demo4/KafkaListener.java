//package com.wangyuxuan.kafka.demo4;
//
//import kafka.common.TopicAndPartition;
//import org.apache.spark.streaming.kafka.KafkaCluster;
//import org.apache.spark.streaming.kafka.OffsetRange;
//import org.apache.spark.streaming.scheduler.*;
//import scala.Option;
//import scala.collection.JavaConversions;
//import scala.collection.immutable.List;
//
//import java.util.HashMap;
//import java.util.Map;
//
///**
// * @author wangyuxuan
// * @date 2020/3/4 16:57
// * @description 监听器
// */
//public class KafkaListener implements StreamingListener {
//
//    private KafkaCluster kc;
//    public scala.collection.immutable.Map<String, String> kafkaParams;
//
//    public KafkaListener(scala.collection.immutable.Map<String, String> kafkaParams) {
//        this.kafkaParams = kafkaParams;
//        kc = new KafkaCluster(kafkaParams);
//    }
//
//    @Override
//    public void onStreamingStarted(StreamingListenerStreamingStarted streamingStarted) {
//
//    }
//
//    @Override
//    public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {
//    }
//
//    @Override
//    public void onReceiverError(StreamingListenerReceiverError receiverError) {
//    }
//
//    @Override
//    public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {
//    }
//
//    @Override
//    public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {
//    }
//
//    @Override
//    public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {
//    }
//
//    /**
//     * 批次完成时调用的方法
//     *
//     * @param batchCompleted batchCompleted 对象里面带有了偏移量的信息，所以我提交偏移量的时候，就是从这个对象里面读取offset就可以了。
//     *                       <p>
//     *                       SparkStreaming -> 5s -> Job -> batch一次
//     *                       task1   100 hbase
//     *                       task2   100 hbase
//     *                       task3   100 hbase
//     *                       运行成功
//     *                       task4  100
//     *                       运行失败的 -》 hbase
//     */
//    @Override
//    public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
//        // 如果本批次里面有任务失败了，那么就终止偏移量提交
//        scala.collection.immutable.Map<Object, OutputOperationInfo> opsMap = batchCompleted.batchInfo().outputOperationInfos();
//        Map<Object, OutputOperationInfo> javaOpsMap = JavaConversions.mapAsJavaMap(opsMap);
//        for (Map.Entry<Object, OutputOperationInfo> entry : javaOpsMap.entrySet()) {
//            // failureReason不等于None(是scala中的None)，说明有异常，不保存offset
//            // task -> failureReason
//            if (!"None".equalsIgnoreCase(entry.getValue().failureReason().toString())) {
//                return;
//            }
//        }
//
//        long batchTime = batchCompleted.batchInfo().batchTime().milliseconds();
//        /**
//         * topic，分区，偏移量
//         */
//        Map<String, Map<Integer, Long>> offset = getOffset(batchCompleted);
//
//        for (Map.Entry<String, Map<Integer, Long>> entry : offset.entrySet()) {
//            String topic = entry.getKey();
//            Map<Integer, Long> paritionToOffset = entry.getValue();
//            // 我只需要在这把偏移信息放入到zookeeper就可以了。
//            for (Map.Entry<Integer, Long> p2o : paritionToOffset.entrySet()) {
//                Map<TopicAndPartition, Object> map = new HashMap<TopicAndPartition, Object>();
//                TopicAndPartition topicAndPartition =
//                        new TopicAndPartition(topic, p2o.getKey());
//                map.put(topicAndPartition, p2o.getValue());
//                scala.collection.immutable.Map<TopicAndPartition, Object>
//                        topicAndPartitionObjectMap = TypeHelper.toScalaImmutableMap(map);
//                // 提交偏移量
//                // 这个方法是 0.8的依赖提供的API
//                kc.setConsumerOffsets(kafkaParams.get("group.id").get(), topicAndPartitionObjectMap);
//
//            }
//        }
//    }
//
//    @Override
//    public void onOutputOperationStarted(StreamingListenerOutputOperationStarted outputOperationStarted) {
//    }
//
//    @Override
//    public void onOutputOperationCompleted(StreamingListenerOutputOperationCompleted outputOperationCompleted) {
//    }
//
//    private Map<String, Map<Integer, Long>> getOffset(StreamingListenerBatchCompleted batchCompleted) {
//        Map<String, Map<Integer, Long>> map = new HashMap<>();
//
//        scala.collection.immutable.Map<Object, StreamInputInfo> inputInfoMap =
//                batchCompleted.batchInfo().streamIdToInputInfo();
//        Map<Object, StreamInputInfo> infos = JavaConversions.mapAsJavaMap(inputInfoMap);
//
//        infos.forEach((k, v) -> {
//            Option<Object> optOffsets = v.metadata().get("offsets");
//            if (!optOffsets.isEmpty()) {
//                Object objOffsets = optOffsets.get();
//                if (List.class.isAssignableFrom(objOffsets.getClass())) {
//                    List<OffsetRange> scalaRanges = (List<OffsetRange>) objOffsets;
//
//                    Iterable<OffsetRange> ranges = JavaConversions.asJavaIterable(scalaRanges);
//                    for (OffsetRange range : ranges) {
//                        if (!map.containsKey(range.topic())) {
//                            map.put(range.topic(), new HashMap<>());
//                        }
//                        map.get(range.topic()).put(range.partition(), range.untilOffset());
//                    }
//                }
//            }
//        });
//
//        return map;
//    }
//}

package com.thoughtworks.injestion;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.thoughtworks.injestion.decoder.CtrlMsgDecoder;
import com.thoughtworks.injestion.decoder.DataMsgDecoder;
import com.thoughtworks.injestion.decoder.MsgIdDecoder;
import com.thoughtworks.message.MessageProtos;
import com.thoughtworks.message.MessageProtos.CtrlMsg;
import com.thoughtworks.message.MessageProtos.DataMsg;
import com.thoughtworks.message.MessageProtos.MsgId;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.util.ManualClock;
import scala.Tuple2;

import java.util.Map;

/**
 * Created by dtong on 20/07/2017.
 */
public class StreamConsumer {
    private static ImmutableMap<String, String> kafkaConf = ImmutableMap.of(
            "bootstrap.servers", "localhost:9092",
            "group.id", "test-test",
            "zookeeper.connect", "localhost:2181");

    static class Mappers {
        public static CompletionChecker ctrlMapper(
                MsgId msgId, Optional<CtrlMsg> ctrlMsgOptional, State<CompletionChecker> ctrlMsgState) {
            Map<String, Integer> stats = ctrlMsgOptional.get().getStatisticsMap();
            CompletionChecker checker =
                    new CompletionChecker(
                            stats.get(MessageProtos.DataType.TRADE_DATA.name()),
                            stats.get(MessageProtos.DataType.MARKET_DATA.name()));
            ctrlMsgState.update(checker);
            return checker;
        }

        public static Tuple2<MsgId, CompletionChecker> dataMapper(
                MsgId msgId, Optional<DataMsg> dataMsg, State<CompletionChecker> state) {
            CompletionChecker checker;
            if (!state.exists()) {
                checker = CompletionChecker.empty();
            } else {
                checker = state.get();
            }
            CompletionChecker updated = checker.consume(dataMsg.get().getType());
            state.update(updated);
            return new Tuple2<>(msgId, updated);
        }
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("StreamAppConsumer")
                .setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        jssc.checkpoint("ckpt");
        JavaPairInputDStream<MsgId, CtrlMsg> ctrlStream = createCtrlStream(jssc);
        JavaPairInputDStream<MsgId, DataMsg> dataStream = createDataStream(jssc);

        JavaMapWithStateDStream<MsgId, CtrlMsg, CompletionChecker, CompletionChecker> ctrlStateStream =
                ctrlStream.mapWithState(StateSpec.function(Mappers::ctrlMapper));
        JavaPairDStream<MsgId, CompletionChecker> ctrlSnapStream = ctrlStateStream.stateSnapshots();

        JavaMapWithStateDStream<MsgId, DataMsg, CompletionChecker, Tuple2<MsgId, CompletionChecker>> dataStateStream =
                dataStream.mapWithState(StateSpec.function(Mappers::dataMapper));

        JavaPairDStream<MsgId, CompletionChecker> dataSnapStream = dataStateStream.stateSnapshots();

        JavaPairDStream<MsgId, CompletionChecker> checkResultStream =
                ctrlSnapStream.join(dataSnapStream).mapToPair(tup -> {
                    MsgId msgId = tup._1();
                    Tuple2<CompletionChecker, CompletionChecker> checkerPair = tup._2();
                    CompletionChecker ctrlState = checkerPair._1();
                    CompletionChecker dataState = checkerPair._2();
                    return new Tuple2<>(msgId, ctrlState.substract(dataState));
                });

        JavaPairDStream<MsgId, CompletionChecker> successStream = checkResultStream
                .filter(tup -> tup._2().isCompleted());

        successStream
                .map(tup -> tup._1().getReqId() + " Finished.")
                .print();

        jssc.start();
        jssc.awaitTermination();
    }

    private static JavaPairInputDStream<MsgId, DataMsg> createDataStream(JavaStreamingContext jssc) {
        return KafkaUtils.createDirectStream(jssc,
                MsgId.class, DataMsg.class,
                MsgIdDecoder.class, DataMsgDecoder.class,
                kafkaConf, ImmutableSet.of("data"));
    }

    private static JavaPairInputDStream<MsgId, CtrlMsg> createCtrlStream(JavaStreamingContext jssc) {
        return KafkaUtils.createDirectStream(jssc,
                MsgId.class, CtrlMsg.class,
                MsgIdDecoder.class, CtrlMsgDecoder.class,
                kafkaConf, ImmutableSet.of("ctrl"));
    }
}

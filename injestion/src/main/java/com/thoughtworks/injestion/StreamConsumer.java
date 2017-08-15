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
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Map;
import java.util.Objects;

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
            if (ctrlMsgOptional.isPresent()) {
                Map<String, Integer> stats = ctrlMsgOptional.get().getStatisticsMap();
                CompletionChecker checker =
                        new CompletionChecker(
                                stats.get(MessageProtos.DataType.TRADE_DATA.name()),
                                stats.get(MessageProtos.DataType.MARKET_DATA.name()));
                ctrlMsgState.update(checker);
                return checker;
            } else {
                if (ctrlMsgState.exists() && !ctrlMsgState.isTimingOut()) {
                    ctrlMsgState.remove();
                }
                return null;
            }
        }

        public static Tuple2<MsgId, CompletionChecker> dataMapper(
                MsgId msgId, java.util.Optional<DataMsg> dataMsg, State<CompletionChecker> state) {
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

        public static Tuple2<MsgId, CompletionChecker> joinMapper(
                MsgId msgId, Optional<Tuple2<Optional<DataMsg>, Optional<CompletionChecker>>> tup,
                State<Tuple2<CompletionChecker, CompletionChecker>> stateTypeState) {
            CompletionChecker dataChecker = tup.get()._1()
                    .transform(CompletionChecker::count)
                    .or(new CompletionChecker(0, 0));
            CompletionChecker ctrlChecker = tup.get()._2()
                    .or(new CompletionChecker(0, 0));

            if (!stateTypeState.exists()) {
                stateTypeState.update(new Tuple2<>(ctrlChecker, dataChecker));
            } else {
                Tuple2<CompletionChecker, CompletionChecker> oldTup = stateTypeState.get();
                CompletionChecker newDataChecker = oldTup._2().plus(dataChecker);
                stateTypeState.update(new Tuple2<>(ctrlChecker, newDataChecker));
            }
            if (stateTypeState.get()._1().substract(stateTypeState.get()._2()).isCompleted()) {
                stateTypeState.remove();
                return new Tuple2<>(msgId, dataChecker);
            } else {
                return null;
            }
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

        // TODO: We can remove the state for this stream.
        JavaMapWithStateDStream<MsgId, CtrlMsg, CompletionChecker, CompletionChecker> ctrlStateStream =
                ctrlStream.mapWithState(StateSpec.function(Mappers::ctrlMapper).timeout(Durations.seconds(20)));
        JavaPairDStream<MsgId, CompletionChecker> ctrlSnapStream = ctrlStateStream.stateSnapshots();

        dataStream.fullOuterJoin(ctrlSnapStream)
                .mapWithState(StateSpec.function(Mappers::joinMapper).timeout(Durations.seconds(20)))
                .filter(Objects::nonNull)
                .map(tup -> tup._1().getReqId() + " Finished.")
                .print();

//        JavaMapWithStateDStream<MsgId, DataMsg, CompletionChecker, Tuple2<MsgId, CompletionChecker>> dataStateStream =
//                dataStream.mapWithState(StateSpec.function(Mappers::dataMapper));
//
//        JavaPairDStream<MsgId, CompletionChecker> dataSnapStream = dataStateStream.stateSnapshots();
//
//        JavaPairDStream<MsgId, CompletionChecker> checkResultStream =
//                ctrlSnapStream.join(dataSnapStream).mapToPair(tup -> {
//                    MsgId msgId = tup._1();
//                    Tuple2<CompletionChecker, CompletionChecker> checkerPair = tup._2();
//                    CompletionChecker ctrlState = checkerPair._1();
//                    CompletionChecker dataState = checkerPair._2();
//                    return new Tuple2<>(msgId, ctrlState.substract(dataState));
//                });
//
//        deltaStream(checkResultStream)
//                .map(tup -> tup._1().getReqId() + " Finished.")
//                .print();

        jssc.start();
        jssc.awaitTermination();
    }

    private static JavaPairDStream<MsgId, Integer> deltaStream(JavaPairDStream<MsgId, CompletionChecker> checkResultStream) {
        return checkResultStream
                .filter(tup1 -> tup1._2().isCompleted())
                .window(Durations.seconds(2))
                .mapValues(cc -> 1)
                .reduceByKey((a, b) -> a + b)
                .filter(tup -> tup._2() == 1);
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

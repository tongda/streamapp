package com.thoughtworks.datagen;

import com.google.common.io.Resources;
import com.thoughtworks.message.MessageProtos.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

/**
 * Created by dtong on 10/07/2017.
 */
public class StreamProducer {
    private static KafkaProducer<MsgId, CtrlMsg> ctrlProducer;
    private static KafkaProducer<MsgId, DataMsg> dataProducer;
    private static MsgId.Builder msgIdBuilder;
    private static CtrlMsg.Builder ctrlBuilder;
    private static DataMsg.Builder dataBuilder;
    private static Random random;

    public StreamProducer() {

    }

    static {
        Properties propsCtrl = new Properties();
        try (InputStream stream = Resources.getResource("producer-ctrl.props").openStream()) {
            propsCtrl.load(stream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Properties propsData = new Properties();
        try (InputStream stream = Resources.getResource("producer-data.props").openStream()) {
            propsData.load(stream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        ctrlProducer = new KafkaProducer<>(propsCtrl);
        dataProducer = new KafkaProducer<>(propsData);
        msgIdBuilder = MsgId.newBuilder();
        ctrlBuilder = CtrlMsg.newBuilder();
        dataBuilder = DataMsg.newBuilder();
        random = new Random();
    }

    private static MsgId generateId(int id) {
        String reqId = "REQ-" + id;
        String foId = "FO-" + id;

        msgIdBuilder.setReqId(reqId);
        msgIdBuilder.setFOId(foId);

        return msgIdBuilder.build();
    }

    private static ProducerRecord<MsgId, CtrlMsg> generateCtrlMsg(MsgId id, String foName,
                                                                  int numTrades, int numMarketData) {
        ctrlBuilder.setFOName(foName);
        ctrlBuilder.putStatistics(DataType.TRADE_DATA.name(), numTrades);
        ctrlBuilder.putStatistics(DataType.MARKET_DATA.name(), numMarketData);
        return new ProducerRecord<>("ctrl", id, ctrlBuilder.build());
    }

    private static ProducerRecord<MsgId, DataMsg> generateDataMsg(MsgId msgId, int tradeId, DataType type) {
        dataBuilder.setId("Trade-" + tradeId);
        Integer value = random.nextInt(100);
        dataBuilder.setValue(value.toString());
        dataBuilder.setType(type);
        return new ProducerRecord<>("data", msgId, dataBuilder.build());
    }

    public static void main(String[] argv) {
        for (int i = 0; i < 10; i++) {
            MsgId msgId = generateId(i);
            try {
                int numTrades = random.nextInt(10);
                int numMarketData = random.nextInt(10);
                ctrlProducer.send(generateCtrlMsg(msgId, "fo-" + i % 10, numTrades, numMarketData));

                for (int j = 0; j < numTrades; j++) {
                    dataProducer.send(generateDataMsg(msgId, j, DataType.TRADE_DATA));

                    Thread.sleep(random.nextInt(1000));
                }

                for (int j = 0; j < numMarketData; j++) {
                    dataProducer.send(generateDataMsg(msgId, j, DataType.MARKET_DATA));
                    Thread.sleep(random.nextInt(1000));
                }
                System.out.println("Data committed: " + i);
            } catch (KafkaException ex) {
                ex.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

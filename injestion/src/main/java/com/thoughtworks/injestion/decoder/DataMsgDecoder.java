package com.thoughtworks.injestion.decoder;

import com.google.protobuf.InvalidProtocolBufferException;
import com.thoughtworks.message.MessageProtos;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

/**
 * Created by dtong on 20/07/2017.
 */
public class DataMsgDecoder implements Decoder<MessageProtos.DataMsg> {
    public DataMsgDecoder(VerifiableProperties properties) {
    }

    @Override
    public MessageProtos.DataMsg fromBytes(byte[] bytes) {
        try {
            return MessageProtos.DataMsg.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            return null;
        }
    }
}

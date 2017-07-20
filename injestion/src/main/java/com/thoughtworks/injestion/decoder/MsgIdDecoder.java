package com.thoughtworks.injestion.decoder;

import com.google.protobuf.InvalidProtocolBufferException;
import com.thoughtworks.message.MessageProtos;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

/**
 * Created by dtong on 19/07/2017.
 */
public class MsgIdDecoder implements Decoder<MessageProtos.MsgId> {
    public MsgIdDecoder(VerifiableProperties properties) {
    }

    @Override
    public MessageProtos.MsgId fromBytes(byte[] bytes) {
        try {
            return MessageProtos.MsgId.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            return null;
        }
    }
}

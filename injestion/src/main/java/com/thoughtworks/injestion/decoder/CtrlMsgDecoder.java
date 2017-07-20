package com.thoughtworks.injestion.decoder;

import com.google.protobuf.InvalidProtocolBufferException;
import com.thoughtworks.message.MessageProtos;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

/**
 * Created by dtong on 19/07/2017.
 */
public class CtrlMsgDecoder implements Decoder<MessageProtos.CtrlMsg> {
    public CtrlMsgDecoder(VerifiableProperties properties) {
    }

    @Override
    public MessageProtos.CtrlMsg fromBytes(byte[] bytes) {
        try {
            return MessageProtos.CtrlMsg.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            return null;
        }
    }
}

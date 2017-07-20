package com.thoughtworks.datagen.serialization;

import com.google.protobuf.GeneratedMessageV3;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Created by dtong on 11/07/2017.
 */
public class ProtobufSerializer implements Serializer {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data instanceof GeneratedMessageV3) {
            GeneratedMessageV3 casted = (GeneratedMessageV3) data;
            return casted.toByteArray();
        } else {
            return null;
        }
    }

    @Override
    public void close() {

    }
}

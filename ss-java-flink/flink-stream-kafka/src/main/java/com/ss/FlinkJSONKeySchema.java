package com.ss;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

/**
 * Created by zhaozh on 2021/06/17.
 */
public class FlinkJSONKeySchema {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "latest");
        // 指定Kafka的连接位置
        properties.setProperty("bootstrap.servers", "hadoop001:9092");

        // 指定监听的主题，并定义Kafka字节消息到Flink对象之间的转换规则
        properties.setProperty("group.id", "test");

        try {


        DataStream<ObjectNode> stream = env
                .addSource(new FlinkKafkaConsumer<>("flink-stream-in-topic", new JSONKeyValueDeserializationSchema(true), properties));

        stream.print();

        env.execute("Flink Streaming");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

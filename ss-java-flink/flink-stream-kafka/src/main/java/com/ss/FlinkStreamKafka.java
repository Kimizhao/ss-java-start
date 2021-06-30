package com.ss;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Created by zhaozh on 2021/06/17.
 */
public class FlinkStreamKafka {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        // 指定Kafka的连接位置
        properties.setProperty("bootstrap.servers", "hadoop001:9092");

        // 指定监听的主题，并定义Kafka字节消息到Flink对象之间的转换规则
        properties.setProperty("group.id", "test");

        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("flink-stream-in-topic", new SimpleStringSchema(), properties));

        stream.print();

        try {
            env.execute("Flink Streaming");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

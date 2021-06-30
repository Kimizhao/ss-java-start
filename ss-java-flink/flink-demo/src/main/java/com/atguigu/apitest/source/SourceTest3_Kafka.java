package com.atguigu.apitest.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Created by zhaozh on 2021/06/26.
 * ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sensor
 */
public class SourceTest3_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        //properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //properties.setProperty("auto.offset.reset", "latest");
        //properties.setProperty("group.id", "test");
        // 指定Kafka的连接位置
        properties.setProperty("bootstrap.servers", "hadoop001:9092");

        // 从kafka读取数据
        DataStream<String> dataStream = env.addSource( new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties));

        // 打印输出
        dataStream.print();

        // 执行
        env.execute();
    }
}

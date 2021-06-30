package com.atguigu.apitest.sink;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * Created by zhaozh on 2021/06/26.
 * ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sinktest
 *
 * kafka进 kafka出
 * ETL过程
 */
public class SinkTest1_Kafka2 {
    public static void main(String[] args) throws Exception{
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
        DataStream<String> inputStream = env.addSource( new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties));

        DataStream<String> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2])).toString();
        });

        dataStream.addSink(new FlinkKafkaProducer<String>("hadoop001:9092", "sinktest", new SimpleStringSchema()));


        env.execute();
    }
}

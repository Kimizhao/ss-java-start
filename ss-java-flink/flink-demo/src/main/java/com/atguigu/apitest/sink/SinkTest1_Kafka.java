package com.atguigu.apitest.sink;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * Created by zhaozh on 2021/06/26.
 * ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sinktest
 */
public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("J:\\700-repo\\github\\Kimizhao\\ss-java-start\\ss-java-flink\\flink-demo\\src\\main\\resources\\sensor1.txt");

        DataStream<String> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2])).toString();
        });

        dataStream.addSink(new FlinkKafkaProducer<String>("hadoop001:9092", "sinktest", new SimpleStringSchema()));


        env.execute();
    }
}

package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by zhaozh on 2021/06/26.
 */
public class TransformTest6_Partition {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("J:\\700-repo\\github\\Kimizhao\\ss-java-start\\ss-java-flink\\flink-demo\\src\\main\\resources\\sensor1.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.print("input");

        // 1. shuffle 重分区，发牌
        DataStream<String> shuffleStream = inputStream.shuffle();


        shuffleStream.print("shuffleStream");

        // 2. keyBy 根据hash code 分区
        dataStream.keyBy("id").print("keyBy");

        // 3. global 把所有数据发送到下游第一个分区
        dataStream.global().print("global");

        env.execute();
    }
}

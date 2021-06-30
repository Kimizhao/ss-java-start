package com.ss;

import com.alibaba.fastjson.JSON;
import com.ss.model.Gb32960Track;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Created by zhaozh on 2021/06/17.
 */
@Slf4j
public class FlinkStreamWindow {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        // 指定Kafka的连接位置
        properties.setProperty("bootstrap.servers", "hadoop001:9092");

        // 指定监听的主题，并定义Kafka字节消息到Flink对象之间的转换规则
        properties.setProperty("group.id", "test");


        DataStream<Gb32960Track> stream = env
                .addSource(new FlinkKafkaConsumer<>("gb32960_track", new SimpleStringSchema(), properties))
                .setParallelism(1)
                .map(new MapFunction<String, Gb32960Track>() {
                    @Override
                    public Gb32960Track map(String s) throws Exception {
                        if (s.isEmpty()) {
                            log.debug("em");
                        }
                        return JSON.parseObject(s, Gb32960Track.class);
                    }
                }).filter(new RichFilterFunction<Gb32960Track>() {
                    @Override
                    public boolean filter(Gb32960Track gb32960Track) throws Exception {
                        if (gb32960Track.vin != null) {
                            return true;
                        }
                        else {
                            return false;
                        }
                    }
                });

        //stream.print();

        DataStream<Tuple2<String, Integer>> stream1 =  stream
                .keyBy(gb32960Track -> gb32960Track.vin)
                .flatMap(new FlatMapFunction<Gb32960Track, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(Gb32960Track gb32960Track, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        collector.collect(new Tuple2<String, Integer>(gb32960Track.vin, 1));
                    }
                });

        stream1.print();

        try {
            env.execute("Flink Streaming");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

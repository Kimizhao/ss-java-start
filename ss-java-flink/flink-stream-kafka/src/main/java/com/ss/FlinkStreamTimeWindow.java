package com.ss;

import com.alibaba.fastjson.JSON;
import com.ss.model.Gb32960Track;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Properties;

/**
 * Created by zhaozh on 2021/06/17.
 */
@Slf4j
public class FlinkStreamTimeWindow {
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
                        if (gb32960Track.getVin() != null) {
                            return true;
                        }
                        else {
                            return false;
                        }
                    }
                }).keyBy(gb32960Track -> gb32960Track.getVin());

        stream.print();

        DataStream<Tuple2<String, Long>> stream1 =  stream
                .flatMap(new FlatMapFunction<Gb32960Track, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(Gb32960Track gb32960Track, Collector<Tuple2<String,Long>> collector) throws Exception {
                        collector.collect(new Tuple2<String, Long>(gb32960Track.getVin(), gb32960Track.getGpsTime().getTime()));
                    }
                })
                .keyBy(stringDateTuple2 -> stringDateTuple2.f0).countWindow(3).reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> stringDateTuple2, Tuple2<String, Long> t1) throws Exception {
                        if (stringDateTuple2.f1 > t1.f1) {
                            return new Tuple2<>(stringDateTuple2.f0, stringDateTuple2.f1);
                        }
                        else {
                            return new Tuple2<>(stringDateTuple2.f0, t1.f1);
                        }
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

package com.ss.com.ss.apptest;

import com.alibaba.fastjson.JSON;
import com.ss.FlinkStreamKafka2;
import com.ss.model.Gb32960Track;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Properties;

/**
 * Created by zhaozh on 2021/07/01.
 */
@Slf4j
public class VehicleTest1_Redis {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        // 指定Kafka的连接位置
        properties.setProperty("bootstrap.servers", "hadoop001:9092");

        // 指定监听的主题，并定义Kafka字节消息到Flink对象之间的转换规则
        //properties.setProperty("group.id", "test");

        DataStream<Gb32960Track> dataStream = env
                .addSource(new FlinkKafkaConsumer<>("gb32960_track", new SimpleStringSchema(), properties))
                .map(new MyMapFunction())
                .filter(new MyRichFilterFunction());

        // Jedis 配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("hadoop001").setPort(6379).build();
        dataStream.addSink(new RedisSink<>(config, new MyRedisMapper()));

        //dataStream.print("Vehicle data");

        env.execute("Vehicle data sink to redis.");

    }

    public static class MyMapFunction implements MapFunction<String, Gb32960Track> {
        @Override
        public Gb32960Track map(String s) throws Exception {
            Gb32960Track gb32960Track = JSON.parseObject(s, Gb32960Track.class);
            gb32960Track.setData(s);
            return gb32960Track;
        }
    }

    public static class MyRichFilterFunction extends RichFilterFunction<Gb32960Track> {

        @Override
        public boolean filter(Gb32960Track gb32960Track) throws Exception {
            if (gb32960Track.getVin() != null) {
                return true;
            } else {
                return false;
            }
        }
    }

    public static class MyRedisMapper implements RedisMapper<Gb32960Track> {
        // 定义保存数据到redis的命令，存储Hash表，hset vehicle_gb32960_track_current vin data
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "vehicle_gb32960_track_current");
        }

        @Override
        public String getKeyFromData(Gb32960Track gb32960Track) {
            return gb32960Track.getVin();
        }

        @Override
        public String getValueFromData(Gb32960Track gb32960Track) {
            return gb32960Track.getData();
        }
    }
}

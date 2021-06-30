package com.atguigu.apitest.sink;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * Created by zhaozh on 2021/06/26.
 *127.0.0.1:6379> HGET sensor_temp sensor_1
 * "35.8"
 * 127.0.0.1:6379> HGETALL sensor_temp
 * 1) "sensor_1"
 * 2) "35.8"
 * 3) "sensor_6"
 * 4) "15.4"
 * 5) "sensor_7"
 * 6) "6.7"
 * 7) "sensor_10"
 * 8) "38.1"
 * 127.0.0.1:6379>
 */
public class SinkTest2_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("J:\\700-repo\\github\\Kimizhao\\ss-java-start\\ss-java-flink\\flink-demo\\src\\main\\resources\\sensor1.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // Jedis 配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("hadoop001").setPort(6379).build();
        dataStream.addSink(new RedisSink<>(config, new MyRedisMapper()));

        env.execute();
    }

    public static class MyRedisMapper implements RedisMapper<SensorReading> {
        // 定义保存数据到redis的命令，存储Hash表，hset sensor_temp id temperature
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "sensor_temp");
        }

        @Override
        public String getKeyFromData(SensorReading sensorReading) {
            return sensorReading.getId();
        }

        @Override
        public String getValueFromData(SensorReading sensorReading) {
            return sensorReading.getTemperature().toString();
        }
    }
}

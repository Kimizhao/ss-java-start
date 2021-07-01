package com.ss.com.ss.apptest;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ss.utils.JsonConvertKeyUtil;
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
public class VehicleTest2_Redis {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        Properties properties = new Properties();
        // 指定Kafka的连接位置
        properties.setProperty("bootstrap.servers", "hadoop001:9092");

        // 指定监听的主题，并定义Kafka字节消息到Flink对象之间的转换规则
        //properties.setProperty("group.id", "test");

        //
        DataStream<JSONObject> dataStream = env
                .addSource(new FlinkKafkaConsumer<>("gb32960_track", new SimpleStringSchema(), properties))
                .map(new MyMapFunction())
                .filter(new MyRichFilterFunction());

        // Jedis 配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("hadoop001").setPort(6379).build();
        dataStream.addSink(new RedisSink<>(config, new MyRedisMapper()));

        DataStream<JSONObject> dataStatusStream = dataStream.map(new MapStatusFunction());
        dataStatusStream.addSink(new RedisSink<>(config, new StatusRedisMapper()));

        //dataStream.print("Vehicle data");

        env.execute("Vehicle data sink to redis.");

    }

    public static class MyMapFunction implements MapFunction<String, JSONObject> {
        @Override
        public JSONObject map(String s) throws Exception {
            JSONObject gb32960Track = JSON.parseObject(s);
            gb32960Track = JsonConvertKeyUtil.covertObjectToString(gb32960Track);
            if (gb32960Track.containsKey("gps_time")){
                long lGpsTime = DateUtil.parse(gb32960Track.getString("gps_time")).getTime();
                gb32960Track.put("gps_time", lGpsTime);
            }

            return gb32960Track;
        }
    }

    public static class MapStatusFunction implements MapFunction<JSONObject, JSONObject> {
        @Override
        public JSONObject map(JSONObject s) throws Exception {
            JSONObject statusOjbect = new JSONObject();

            if (s.containsKey("vin")) {
                statusOjbect.put("vin", s.getString("vin"));
            }

            if (s.containsKey("gps_time")) {
                statusOjbect.put("gps_time", s.getLong("gps_time"));
            }

            // 是否在线
            statusOjbect.put("is_online", 1);

            // 是否未定位
            if (s.containsKey("locate_statu")) {
                statusOjbect.put("is_nolocate", s.getByte("locate_statu"));
            }

            // 是否有CAN
            if (s.containsKey("ext_flag")) {
                statusOjbect.put("is_extflag", s.getByte("ext_flag"));
            }

            // 是否在充电
            if (s.containsKey("charge_status")) {
                statusOjbect.put("is_charge", s.getByte("charge_status") == 1 || s.getByte("charge_status") == 2 ? 1 : 0);
            }

            // 是否在报警
            statusOjbect.put("is_alarm", 0);

            // 累计里程
            if (s.containsKey("mileage")) {
                statusOjbect.put("mileage", s.getDouble("mileage"));
            }

            // 启动状态
            if (s.containsKey("car_status")) {
                statusOjbect.put("start_status", s.getDouble("car_status"));
            }

            // 车辆是否行驶
            if (s.containsKey("speed")) {
                statusOjbect.put("is_on_drive", s.getDouble("speed") > 0.0 ? 1 : 0);
            }

            // 纬度
            if (s.containsKey("lat")) {
                statusOjbect.put("lat", s.getLong("lat"));
            }

            // 经度
            if (s.containsKey("lng")) {
                statusOjbect.put("lng", s.getLong("lng"));
            }

            return statusOjbect;
        }
    }

    public static class MyRichFilterFunction extends RichFilterFunction<JSONObject> {

        @Override
        public boolean filter(JSONObject gb32960Track) throws Exception {
            if (!gb32960Track.containsKey("vin"))
            {
                return false;
            }

            String vin = gb32960Track.getString("vin");
            if (vin != null) {
                return true;
            } else {
                return false;
            }
        }
    }

    public static class MyRedisMapper implements RedisMapper<JSONObject> {
        // 定义保存数据到redis的命令，存储Hash表，hset vehicle_gb32960_track_current vin data
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "vehicle_gb32960_track_current");
        }

        @Override
        public String getKeyFromData(JSONObject gb32960Track) {
            String vin = gb32960Track.getString("vin");
            return vin;
        }

        @Override
        public String getValueFromData(JSONObject gb32960Track) {
            return gb32960Track.toJSONString();
        }
    }

    public static class StatusRedisMapper implements RedisMapper<JSONObject> {
        // 定义保存数据到redis的命令，存储Hash表，hset vehicle_gb32960_track_current vin data
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "vehicle_gb32960_status");
        }

        @Override
        public String getKeyFromData(JSONObject gb32960Track) {
            String vin = gb32960Track.getString("vin");
            return vin;
        }

        @Override
        public String getValueFromData(JSONObject gb32960Track) {
            return gb32960Track.toJSONString();
        }
    }
}

package com.ss.sprintflink;

import cn.hutool.core.date.DateUtil;
//import com.mongodb.util.JSON;
//import com.mongodb.DBObject;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ss.sprintflink.sinks.SinkToMongodb;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import scala.Tuple3;
import scala.util.parsing.json.JSONObject$;

import java.util.Properties;

@SpringBootApplication
public class SprintFlinkApplication  implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(SprintFlinkApplication.class, args);
    }

    @Override
    public void run(String... args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>("gb32960_track", new SimpleStringSchema(), getProperties());
        DataStream<String> dataStream = env.addSource(flinkKafkaConsumer);

        dataStream.print("gb32960_track");

/*      dataStream.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        });*/

        DataStream<Tuple3<String, Long, JSONObject>> dataStream1 = dataStream.flatMap(new FlatMapFunction<String, Tuple3<String, Long, JSONObject>>() {
            @Override
            public void flatMap(String s, Collector<Tuple3<String, Long, JSONObject>> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);

                if (jsonObject.containsKey("Vin") && jsonObject.containsKey("GpsTime")) {
                    String vin = jsonObject.getString("Vin");
                    Long lGpsTime = DateUtil.parse(jsonObject.getString("GpsTime")).getTime();
                    collector.collect(new Tuple3<String, Long, JSONObject>(vin, lGpsTime, jsonObject));
                }
            }
        });

        /*DataStream<Tuple3<String, Long, DBObject>> dataStream1 = dataStream.flatMap(new FlatMapFunction<String, Tuple3<String, Long, DBObject>>() {
            @Override
            public void flatMap(String s, Collector<Tuple3<String, Long, DBObject>> collector) throws Exception {
                DBObject dbObject = (DBObject) JSON.parse(s);

                if (dbObject.containsField("Vin") && dbObject.containsField("GpsTime")) {
                    String vin = dbObject.get("Vin").toString();
                    Long lGpsTime = DateUtil.parse(dbObject.get("GpsTime").toString()).getTime();
                    collector.collect(new Tuple3<String, Long, DBObject>(vin, lGpsTime, dbObject));
                }
            }
        });*/

        //dataStream1.print("gb32960_track_ojbect");
        dataStream1.addSink(new SinkToMongodb());


        // 此处省略处理逻辑
        //dataStream.addSink(new MySink());

        try {
            env.execute("Sprint Flink Streaming");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop001:9092");
        //properties.setProperty("zookeeper.connect", "hadoop001:2181");
        properties.setProperty("group.id", "test");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }

}

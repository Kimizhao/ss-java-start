package com.atguigu.apitest.sink;

import com.atguigu.apitest.beans.SensorReading;
import com.atguigu.apitest.source.SourceTest4_UDF;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Created by zhaozh on 2021/06/26.
 * 自定义 sink
 * mysql -u root -p
 * mysql> show tables;
 * Empty set
 *
 * mysql> create table sensor_temp (
 *     -> id varchar(20) not null,
 *     -> temp double not null);
 * Query OK, 0 rows affected (4.41 sec)
 *
 * mysql> show tables;
 * +----------------+
 * | Tables_in_test |
 * +----------------+
 * | sensor_temp    |
 * +----------------+
 * 1 row in set (0.03 sec)
 *
 * mysql>
 */
public class SinkTest4_Jdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        //DataStream<String> inputStream = env.readTextFile("J:\\700-repo\\github\\Kimizhao\\ss-java-start\\ss-java-flink\\flink-demo\\src\\main\\resources\\sensor1.txt");
//        DataStream<SensorReading> dataStream = inputStream.map(line -> {
//            String[] fields = line.split(",");
//            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
//        });

        DataStream<SensorReading> dataStream = env.addSource(new SourceTest4_UDF.MySensorSource());


        dataStream.addSink(new MyJdbcSink());

        env.execute();
    }

    // 实现自定的SinkFunction
    public static class MyJdbcSink extends RichSinkFunction<SensorReading> {
        // 声明连接
        private Connection connection = null;

        // 优化：声明预编译语句，执行计划
        private PreparedStatement insertStmt = null;
        private PreparedStatement updateStmt = null;


        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://mysql-master.ygai:23306/test?useSSL=false","root","123qwe");

            insertStmt = connection.prepareStatement("insert into sensor_temp (id, temp) values (?, ?)");
            updateStmt = connection.prepareStatement("update sensor_temp set temp = ? where id = ?");
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            //每来一条数据，调用连接，执行sql

            // 直接执行更新语句，如果没有更新那么就插入

            updateStmt.setDouble(1, value.getTemperature());
            updateStmt.setString(2, value.getId());

            updateStmt.execute();

            if (updateStmt.getUpdateCount() == 0){
                insertStmt.setString(1, value.getId());
                insertStmt.setDouble(2,value.getTemperature());
                insertStmt.execute();
            }
        }
    }
}

package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by zhaozh on 2021/06/26.
 */
public class TransformTest5_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //env.setParallelism(4);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("J:\\700-repo\\github\\Kimizhao\\ss-java-start\\ss-java-flink\\flink-demo\\src\\main\\resources\\sensor1.txt");

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //
        DataStream<Tuple2<String, Integer>> resultStream = dataStream.map(new MyMapper());
        resultStream.print();

        env.execute();
    }

    public static class MyMapper0 implements MapFunction<SensorReading, Tuple2<String, Integer>>{
        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
            return new Tuple2<>(sensorReading.getId(), sensorReading.getId().length());
        }
    }

    public static class MyMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>>{
        @Override
        public void open(Configuration parameters) throws Exception {
            //super.open(parameters);
            // 初始化工作，一般是定义状态，或者建立数据库连接
            // 调用实例与并行数一致
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            //super.close();
            // 一般是关闭连接或者清空状态
            System.out.println("close");
        }

        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
            //可以获取各种各样状态
            //getRuntimeContext().getState()
            return new Tuple2<>(sensorReading.getId(), sensorReading.getId().length());
        }
    }

}

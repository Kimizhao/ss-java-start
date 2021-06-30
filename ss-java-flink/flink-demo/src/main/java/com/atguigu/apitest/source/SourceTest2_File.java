package com.atguigu.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by zhaozh on 2021/06/26.
 */
public class SourceTest2_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从文件读取数据
        DataStream<String> dataStream = env.readTextFile("J:\\700-repo\\github\\Kimizhao\\ss-java-start\\ss-java-flink\\flink-demo\\src\\main\\resources\\sensor.txt");

        // 打印输出
        dataStream.print();

        // 执行
        env.execute();
    }
}

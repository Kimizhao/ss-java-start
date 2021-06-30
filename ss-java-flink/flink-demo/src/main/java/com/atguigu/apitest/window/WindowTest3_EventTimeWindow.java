package com.atguigu.apitest.window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by zhaozh on 2021/06/30.
 */
public class WindowTest3_EventTimeWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
       //env.setStreamTimeCharacteristic();
    }
}

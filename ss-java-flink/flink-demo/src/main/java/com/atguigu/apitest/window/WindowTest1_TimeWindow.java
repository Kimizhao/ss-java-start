package com.atguigu.apitest.window;

import cn.hutool.core.lang.Tuple;
import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Created by zhaozh on 2021/06/29.
 */
public class WindowTest1_TimeWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
/*        DataStream<String> inputStream = env.readTextFile("J:\\700-repo\\github\\Kimizhao\\ss-java-start\\ss-java-flink\\flink-demo\\src\\main\\resources\\sensor1.txt");
        */

        // socket 文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 增量聚合
        DataStream<Integer> resultStream = dataStream.keyBy(data -> data.getId())
                //.timeWindow(Time.seconds(15))
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(15)))//滚动时间窗口
//        .window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(15)))//滑动时间窗口
                //.window(EventTimeSessionWindows.withGap(Time.minutes(10)))
                //.countWindow(5)//滚动计数
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading sensorReading, Integer integer) {
                        return integer + 1;
                    }

                    @Override
                    public Integer getResult(Integer integer) {
                        return integer;
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        return integer + acc1;
                    }
                });

        // 全窗口函数
/*        DataStream<Tuple3<String, Long, Integer>> resultStrem2 = dataStream.keyBy(data -> data.getId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .apply(new WindowFunction<SensorReading, Integer, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple objects, TimeWindow window, Iterable<SensorReading> input, Collector<Integer> out) throws Exception {


                    }
                });*/

        // 3. 其他可选API
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late");

        dataStream.keyBy(data -> data.getId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                //.trigger()
        //.evictor()
        .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .sum("");


        resultStream.print();

    }
}

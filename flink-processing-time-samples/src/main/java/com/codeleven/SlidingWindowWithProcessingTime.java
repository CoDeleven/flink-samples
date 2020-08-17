package com.codeleven;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * 最通用的ProcessingTime+SlidingWindow的使用方式
 * 前置条件：
 * 1. normal.txt
 * 2. 时间窗口大小为5秒，Slide为2秒，即每隔两秒创建一个窗口
 * 3. 窗口的聚合函数被执行后，输出该窗口的范围
 *
 * 写了EventTime和ProcessingTime关于滚动、滑动窗口的demo之后，其实感觉窗口的种类理解了就好
 * 具体的什么时候触发，怎么处理   是由Trigger决定的
 * 撇开Trigger，EventTime和ProcessingTime只是影响窗口的分配
 * 窗口该多大还是多大
 */
public class SlidingWindowWithProcessingTime {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置时间语义。即使不设置，默认情况下也是ProcessingTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        String normalFilePath = WindowWithProcessingTime.class.getResource("/normal.txt").getPath();
        // 请求一行行单线程读入
        DataStreamSource<String> dataStreamSource = env.readTextFile(normalFilePath).setParallelism(1);

        // 拆分字符串，第一个是组，第二个是时间戳，第三个是值
        DataStream<Tuple3<String, Long, Integer>> tuple3DataStream = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple3<String, Long, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                String[] data = value.split(",");
                out.collect(new Tuple3<>(data[0], System.currentTimeMillis(), Integer.parseInt(data[1])));
                Thread.sleep(1000);
            }
        }).setParallelism(1);

        // 注意，这里的DataStream，一定一定要使用assignTimestampsAndWatermarks()方法返回的DataStream
        tuple3DataStream
                // 设置按键分流
                .keyBy(new MyKeySelector())
                // 设置窗口大小为5秒
                .timeWindow(Time.seconds(5), Time.seconds(2))
                .process(new MyProcessFunction()).setParallelism(1)
                .print("time-window").setParallelism(1);

        // 进入一个元素打印一个元素
        tuple3DataStream.print("input").setParallelism(1);
        // 执行任务
        env.execute();
    }

    private static class MyKeySelector implements KeySelector<Tuple3<String, Long, Integer>, String> {
        @Override
        public String getKey(Tuple3<String, Long, Integer> value) throws Exception {
            return value.f0;
        }
    }

    private static class MyProcessFunction extends ProcessWindowFunction<Tuple3<String, Long, Integer>, String, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Tuple3<String, Long, Integer>> elements, Collector<String> out) throws Exception {
            ArrayList<Tuple3<String, Long, Integer>> tuple3List = Lists.newArrayList();
            for (Tuple3<String, Long, Integer> element : elements) {
                tuple3List.add(element);
            }
            // 给该窗口内的数据排序，然后找到第一个数据和最后一个数据，输出窗口范围
            Collections.sort(tuple3List, new Comparator<Tuple3<String, Long, Integer>>() {
                @Override
                public int compare(Tuple3<String, Long, Integer> o1, Tuple3<String, Long, Integer> o2) {
                    return (int) (o1.f1 - o2.f1);
                }
            });

            // 当触发trigger时执行
            Tuple3<String, Long, Integer> first = tuple3List.get(0);
            Tuple3<String, Long, Integer> last = tuple3List.get(tuple3List.size() - 1);

            out.collect("window_range: " + first.f1 + " ~ " + last.f1);
        }
    }
}

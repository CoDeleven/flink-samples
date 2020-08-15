package com.codeleven;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.*;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

/**
 * 证明：在EventTime语义下，当所有的数据输出完了之后，不管水位线有没有、越没越窗口终点，都对所有窗口应用WindowOperator
 * 前置条件：
 * 1. finally_output_all_window.txt
 * 2. 时间窗口大小为5秒
 * 3. 如果数据里，第三个值，也就是value.f2是5的倍数，就创建水位线
 * 4. 窗口的聚合函数被执行后，输出该窗口的范围
 *
 * 剧情介绍：
 * 逻辑窗口范围有两个：
 * 1）1597229875000~1597229879999
 * 2）1597229880000~1597229884999
 *
 * OP1）由于给的所有数据，没有值是5的倍数，所以不会创建水位线，所有数据全都进去，进到各自的窗口范围内
 * OP2）最终会形成两个窗口，当数据全都输入完，EOF了之后，就对所有窗口应用WindowOperator
 *
 * Author: CoDeleven
 * Date: 2020/8/15
 */
public class FinallyOutputAllWindow {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String normalFilePath = WindowWithEventTime.class.getResource("/finally_output_all_window.txt").getPath();
        // 单线程读取，一个个来
        DataStreamSource<String> dataStreamSource = env.readTextFile(normalFilePath).setParallelism(1);

        DataStream<Tuple3<String, Long, Integer>> tuple3DataStream = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple3<String, Long, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                String[] data = value.split(",");
                out.collect(new Tuple3<>(data[0], Long.parseLong(data[1]), Integer.parseInt(data[2])));
                // 慢慢打印，不要一下子全输出完
                Thread.sleep(1000);
            }
        }).setParallelism(1);

        // 设置水位线创建规则
        // 注意，设置完创建规则拿到的是新的DataStream
        DataStream<Tuple3<String, Long, Integer>> newDataStream
                = tuple3DataStream.assignTimestampsAndWatermarks(new MyWaterMarkStrategy());

        newDataStream
                // 设置按键分流
                .keyBy(new MyKeySelector())
                // 设置窗口大小为5秒
                .timeWindow(Time.seconds(5))
                .process(new MyProcessFunction()).setParallelism(1)
                .print("time-window").setParallelism(1);

        // 进入一个元素打印一个元素
        tuple3DataStream.print("input").setParallelism(1);
        // 执行任务
        env.execute();
    }

    private static class MyWaterMarkStrategy implements WatermarkStrategy<Tuple3<String, Long, Integer>> {
        @Override
        public WatermarkGenerator<Tuple3<String, Long, Integer>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new WatermarkGenerator<Tuple3<String, Long, Integer>>() {
                @Override
                public void onEvent(Tuple3<String, Long, Integer> event, long eventTimestamp, WatermarkOutput output) {
                    // 当第三个字段为5的倍数时，生成一个WaterMarker
                    if(event.f2 % 5 == 0){
                        output.emitWatermark(new Watermark(eventTimestamp));
                    }
                }

                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    // 使用了onEvent就不需要实现该接口
                }
            };
        }

        @Override
        public TimestampAssigner<Tuple3<String, Long, Integer>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new TimestampAssigner<Tuple3<String, Long, Integer>>() {
                @Override
                public long extractTimestamp(Tuple3<String, Long, Integer> element, long recordTimestamp) {
                    return element.f1;
                }
            };
        }
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

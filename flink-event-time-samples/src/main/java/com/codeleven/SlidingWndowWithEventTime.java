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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;

/**
 * 最通用的EventTime+SlidingWindow的使用方式
 * 前置条件：
 * 1. normal.txt
 * 2. 时间窗口大小为5秒，Slide为2秒，即每隔两秒创建一个窗口
 * 3. 如果数据里，第三个值，也就是value.f2是5的倍数，就创建水位线
 * 4. 窗口的聚合函数被执行后，输出该窗口的范围
 *
 * 逻辑窗口范围有九个：
 * 1）1597229875000 ~ 1597229876000,2020-08-12 18:57:55~2020-08-12 18:57:56
 * 2）1597229875000 ~ 1597229878000,2020-08-12 18:57:55~2020-08-12 18:57:58
 * 3）1597229876000 ~ 1597229880000,2020-08-12 18:57:56~2020-08-12 18:58:00
 * 4）1597229878000 ~ 1597229882000,2020-08-12 18:57:58~2020-08-12 18:58:02
 * 5）1597229880000 ~ 1597229884999,2020-08-12 18:58:00~2020-08-12 18:58:04
 * 6）1597229882000 ~ 1597229886000,2020-08-12 18:58:02~2020-08-12 18:58:06
 * 7）1597229884000 ~ 1597229888000,2020-08-12 18:58:04~2020-08-12 18:58:08
 * 8）1597229886000 ~ 1597229889000,2020-08-12 18:58:06~2020-08-12 18:58:09
 * 9）1597229888000 ~ 1597229889000,2020-08-12 18:58:08~2020-08-12 18:58:09
 * 可以总结出规律，从第二个窗口起，每次前后时间都是2秒的间隔，也就是创建间隔
 * 当水位线越过窗口终点，就会把所有窗口输出
 *
 * 这里要特别强调一下 窗口1和窗口2，理论情况下，第一个窗口的范围是2020-08-12 18:57:52~2020-08-12 18:57:56
 * 第二个窗口的范围是2020-08-12 18:57:54~2020-08-12 18:57:58。但是我们没有18:57:55，所以就只能输出到18:57:55，看起来就很奇怪
 *
 * 类似的窗口9、窗口8，窗口8的正常范围应该是 2020-08-12 18:58:06~2020-08-12 18:58:10，窗口9的范围是 2020-08-12 18:58:08~2020-08-12 18:58:12
 * 但是因为缺少数据，所以就只能输出到 18:58:09。
 *
 * 注意奥，这是有界数据，无界数据只可能在起点会有类似的问题；
 *
 */
public class SlidingWndowWithEventTime {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String normalFilePath = WindowWithEventTime.class.getResource("/normal.txt").getPath();
        // 请求一行行单线程读入
        DataStreamSource<String> dataStreamSource = env.readTextFile(normalFilePath).setParallelism(1);

        // 拆分字符串，第一个是组，第二个是时间戳，第三个是值
        DataStream<Tuple3<String, Long, Integer>> tuple3DataStream = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple3<String, Long, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                String[] data = value.split(",");
                out.collect(new Tuple3<>(data[0], Long.parseLong(data[1]), Integer.parseInt(data[2])));
                Thread.sleep(1000);
            }
        }).setParallelism(1);

        // 设置水位线创建规则
        // 注意，设置完创建规则拿到的是新的DataStream
        DataStream<Tuple3<String, Long, Integer>> newDataStream
                = tuple3DataStream.assignTimestampsAndWatermarks(new MyWaterMarkStrategy());

        // 注意，这里的DataStream，一定一定要使用assignTimestampsAndWatermarks()方法返回的DataStream
        newDataStream
                // 设置按键分流
                .keyBy(new MyKeySelector())
                // 效果等同于 5秒 的滚动窗口 = .timeWindow(Time.seconds(5))
//                .timeWindow(Time.seconds(5), Time.seconds(5))
                // 设置窗口大小为5秒，创建窗口的频率是每隔两秒创建一次
                .timeWindow(Time.seconds(5), Time.seconds(2))
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
                    if (event.f2 % 5 == 0) {
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

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String firstTime = dateFormat.format(new Date(first.f1));
            String lastTiem = dateFormat.format(new Date(last.f1));
            out.collect("window_range: " + first.f1 + " ~ " + last.f1 + "," + firstTime + "~" + lastTiem);
        }
    }
}

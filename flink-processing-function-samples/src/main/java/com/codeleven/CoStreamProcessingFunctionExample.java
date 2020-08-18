package com.codeleven;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class CoStreamProcessingFunctionExample {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置时间语义。即使不设置，默认情况下也是ProcessingTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        String normalFilePath1 = WrongProcessingFunctionExample.class.getResource("/temperature_normal1.txt").getPath();
        // 请求一行行单线程读入
        DataStreamSource<String> dataStreamSource1 = env.readTextFile(normalFilePath1).setParallelism(1);

        String normalFilePath2 = WrongProcessingFunctionExample.class.getResource("/temperature_normal2.txt").getPath();
        // 请求一行行单线程读入
        DataStreamSource<String> dataStreamSource2 = env.readTextFile(normalFilePath2).setParallelism(1);

        dataStreamSource1.connect(dataStreamSource2)
                .keyBy(new MyKeySelector(), new MyKeySelector())
                .process(new KeyedCoProcessFunction<String, String, String, String>() {
            @Override
            public void processElement1(String value, Context ctx, Collector<String> out) throws Exception {
                System.out.println("1," + value);
                out.collect(value);
            }

            @Override
            public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {
                System.out.println("2," + value);
                out.collect(value);
            }
        }).print();
        /*dataStreamSource1.connect(dataStreamSource2).process(new CoProcessFunction<String, String, Object>() {
            @Override
            public void processElement1(String value, Context ctx, Collector<Object> out) throws Exception {
                // 无法注册定时机制，也无法设置键状态
                ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 5000);
                out.collect(value);
            }

            @Override
            public void processElement2(String value, Context ctx, Collector<Object> out) throws Exception {
                out.collect(value);
            }
        }).print();*/

        // 执行任务
        env.execute();
    }

    private static class MyKeySelector implements KeySelector<String, String> {
        @Override
        public String getKey(String value) throws Exception {
            return value.split(",")[0];
        }
    }

}

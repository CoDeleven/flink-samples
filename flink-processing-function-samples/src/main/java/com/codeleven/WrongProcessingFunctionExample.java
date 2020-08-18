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
import org.apache.flink.util.Collector;

/**
 * 错误的ProcessFunction使用方式！
 * 错误在 对非KeyedStream 使用KeyedState和Timer机制
 * 它们底层不支持，具体源码已经在下方标出
 */
public class WrongProcessingFunctionExample {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置时间语义。即使不设置，默认情况下也是ProcessingTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        String normalFilePath = WrongProcessingFunctionExample.class.getResource("/temperature_normal.txt").getPath();
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

        tuple3DataStream
                // 设置按键分流
//                .keyBy(new MyKeySelector())
                // 设置窗口大小为5秒
                .process(new MyProcessFunction()).setParallelism(1)
                .print("time-window").setParallelism(1);

        // 进入一个元素打印一个元素
        tuple3DataStream.print("in put").setParallelism(1);
        // 执行任务
        env.execute();
    }

    private static class MyKeySelector implements KeySelector<Tuple3<String, Long, Integer>, String> {
        @Override
        public String getKey(Tuple3<String, Long, Integer> value) throws Exception {
            return value.f0;
        }
    }

    private static class MyProcessFunction extends ProcessFunction<Tuple3<String, Long, Integer>, String> {
        ValueState<Integer> temperature;
        ValueState<Long> registerTime;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // ERROR：StreamingRuntimeContext#checkPreconditionsAndGetKeyedStateStore()，不支持获取KeyedState
            temperature = getRuntimeContext().getState(new ValueStateDescriptor<>("temperature", TypeInformation.of(Integer.class)));
        }

        @Override
        public void processElement(Tuple3<String, Long, Integer> value, Context ctx, Collector<String> out) throws Exception {

            if(value.f2 + 2 > temperature.value() && registerTime.value() != null){
                long curTime = System.currentTimeMillis() + 2000;
                // ERROR：ProcessOperator#registerProcessingTimeTimer()，不支持Timer
                ctx.timerService().registerProcessingTimeTimer(curTime);
                registerTime.update(curTime);
            }else{
                out.collect(value.toString());
            }
            temperature.update(value.f2);
        }

    }
}

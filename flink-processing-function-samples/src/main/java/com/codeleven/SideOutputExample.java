package com.codeleven;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * ProcessFunction内的SideOutput输出
 * 前置条件：
 * 1. side_output.txt
 *
 * 在ProcessFunction内使用侧边输出。侧边输出相较于Split，拥有不限类型拆分的优势。Split只能不限数量拆分（按标签），但是类型必须都是一样的。
 */
public class SideOutputExample {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置时间语义。即使不设置，默认情况下也是ProcessingTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        String normalFilePath1 = WrongProcessingFunctionExample.class.getResource("/side_output.txt").getPath();
        // 请求一行行单线程读入
        DataStreamSource<String> dataStreamSource1 = env.readTextFile(normalFilePath1).setParallelism(1);


        final OutputTag<Tuple2<String, Integer>> lowTempTag = new OutputTag<Tuple2<String, Integer>>("low-temperature"){};
        final OutputTag<String> highTempTag = new OutputTag<String>("high-temperature"){};
        final OutputTag<Integer> temperatureStreamTag = new OutputTag<Integer>("just-temperature"){};

        SingleOutputStreamOperator<Tuple2<String, Integer>> source = dataStreamSource1
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] data = value.split(",");
                        return new Tuple2<>(data[0], Integer.parseInt(data[1]));
                    }
                })
                .process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        if (value.f1 > 35) {
                            ctx.output(highTempTag, value.f0 + "," + value.f1);
                        } else {
                            ctx.output(lowTempTag, value);
                        }
                        ctx.output(temperatureStreamTag, value.f1);
                    }
                });
        // 可以获取任意数量的，任意类型的流
        source.getSideOutput(lowTempTag).print("lowTemperature");
        source.getSideOutput(highTempTag).print("highTemperature");
        source.getSideOutput(temperatureStreamTag).print("justTemperature");

        // 执行任务
        env.execute();
    }

    private static class MyKeySelector implements KeySelector<String, String> {
        @Override
        public String getKey(String value) throws Exception {
            return value.split(",")[0];
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
            // ERROR：ProcessOperator#registerProcessingTimeTimer()，不支持Timer
            ctx.timerService().registerProcessingTimeTimer(value.f1);
            registerTime.update(value.f1);
        }

    }
}

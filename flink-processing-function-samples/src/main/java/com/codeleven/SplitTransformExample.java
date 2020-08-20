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
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

/**
 * ProcessFunction内的SideOutput输出
 * 前置条件：
 * 1. side_output.txt
 *
 * 在ProcessFunction内使用侧边输出。侧边输出相较于Split，拥有不限类型拆分的优势。Split只能不限数量拆分（按标签），但是类型必须都是一样的。
 */
public class SplitTransformExample {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置时间语义。即使不设置，默认情况下也是ProcessingTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        String normalFilePath1 = WrongProcessingFunctionExample.class.getResource("/side_output.txt").getPath();
        // 请求一行行单线程读入
        DataStreamSource<String> dataStreamSource1 = env.readTextFile(normalFilePath1).setParallelism(1);


        SplitStream<Tuple2<String, Integer>> source = dataStreamSource1
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] data = value.split(",");
                        return new Tuple2<>(data[0], Integer.parseInt(data[1]));
                    }
                })
                .split(new OutputSelector<Tuple2<String, Integer>>() {
                    @Override
                    public Iterable<String> select(Tuple2<String, Integer> value) {
                        List<String> tags = new ArrayList<>();

                        if (value.f1 > 35) {
                            tags.add("high");
                        } else {
                            tags.add("low");
                        }
                        if("beijing".equals(value.f0)){
                            tags.add("bj");
                        }else if("hangzhou".equals(value.f0)){
                            tags.add("hz");
                        }
                        return tags;
                    }
                });
        // 输出1
//        source.select("high").print();
        // 输出2
        source.select("hz", "high").print();

        // 执行任务
        env.execute();
    }
}

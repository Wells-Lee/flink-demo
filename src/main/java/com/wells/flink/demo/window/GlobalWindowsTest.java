package com.wells.flink.demo.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;

/**
 * Description Global Windows
 * 注意：
 * 1、必须要定义trigger，因为Global Windows 默认的trigger是NeverTrigger；否则不会执行
 * Created by wells on 2020-05-23 20:32:52
 */

public class GlobalWindowsTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String host = "localhost";
        int port = 9999;

        DataStreamSource<String> dataStreamSource = env.socketTextStream(host, port);
        dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        })
                .keyBy(0)
                .window(GlobalWindows.create())
                // 必须指定trigger
                .trigger(CountTrigger.of(2))
                .sum(1)
                .print();

        env.execute();
    }
}

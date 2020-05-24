package com.wells.flink.demo.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Description 计数窗口，分为：
 * 1、滚动计数: 计数就是当某个元素重复出现达到size时才输出，否则不输出(测试时，需要重复输入元素)
 * 2、滑动计数
 * Created by wells on 2020-05-23 20:52:45
 */

public class CountWindowsTest {
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
                // 滚动计数
//                .countWindow(2)
                // 滑动计数
                .countWindow(3, 2)
                .sum(1)
                .print();

        env.execute();
    }
}

package com.wells.flink.demo.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Description Flink流处理
 * Created by wells on 2020-03-17 19:10:17
 */

public class StreamingTest {
    public static void main(String[] args) throws Exception {
        if (null == args || args.length != 2) {
            System.out.println("please check param, Usage: host port");
            return;
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream(host, port);
        SingleOutputStreamOperator<Tuple2<String, Integer>> counts = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }).keyBy(0).sum(1);

        counts.print();

        env.execute();
    }
}

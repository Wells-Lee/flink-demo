package com.wells.flink.demo.window;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Description 
 * Created by wells on 2020-04-29 10:03:22
 */

public class TumblingWindowTest {
    public static void main(String[] args) {
        String host = "127.0.0.1";
        int port = 8090;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.socketTextStream(host, port);

//        SingleOutputStreamOperator<Object> countStream = dataStream
//                .timeWindowAll(Time.of(5, TimeUnit.SECONDS))
//                .flatMap((line, collector) -> {
//                    List<Tuple2<String, Integer>> list = new ArrayList<>();
//                    String[] words = line.split(" ");
//                    if (words.length == 0) {
//                        collector.collect(null);
//                    }
//
//                    for (String word : words) {
//                        collector.collect(new Tuple2<>(word, 1));
//                    }
//                }).filter(Objects::nonNull).keyBy(0).sum(1);
    }
}

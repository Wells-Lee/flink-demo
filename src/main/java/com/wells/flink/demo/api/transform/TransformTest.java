package com.wells.flink.demo.api.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Description 
 * Created by wells on 2020-05-11 19:14:19
 */

public class TransformTest {
    public static void main(String[] args) throws Exception {
        TransformTest transformTest = new TransformTest();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        transformTest.map(env);
//        transformTest.flatMap(env);
//        transformTest.filter(env);
//        transformTest.keyBy(env);
        transformTest.reduce(env);

        env.execute();
    }

    private void reduce(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("/Users/wells/Projects/04-GitHub/java/flink-demo/src/main/resources/wordCountFile.txt");
        source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }).keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) throws Exception {

                return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
            }
        }).print();
    }

    private void keyBy(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("/Users/wells/Projects/04-GitHub/java/flink-demo/src/main/resources/wordCountFile.txt");
        KeyedStream<Tuple2<String, Integer>, Tuple> tupleKeyedStream = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }).keyBy(0);

        tupleKeyedStream.print();
    }

    public void filter(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("/Users/wells/Projects/04-GitHub/java/flink-demo/src/main/resources/wordCountFile.txt");
        source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String line) throws Exception {
                return line.toLowerCase().contains("java");
            }
        }).print().setParallelism(1);
    }

    public void flatMap(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("/Users/wells/Projects/04-GitHub/java/flink-demo/src/main/resources/wordCountFile.txt");
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(" ");
                collector.collect(new Tuple2<>(words[0], 1));
            }
        });

        flatMap.print();
    }

    /**
     * @desc 其中输入是一个数据流，输出的也是一个数据流
     * @method mapTest
     * @param env
     * @return void
     * @date 2020-05-11 19:19:07
     * @author wells
     */
    public void map(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("/Users/wells/Projects/04-GitHub/java/flink-demo/src/main/resources/wordCountFile.txt");
        SingleOutputStreamOperator<String> map = source.map(new MapFunction<String, String>() {
            @Override
            public String map(String line) throws Exception {
                return line;
            }
        });

        map.print();
    }
}

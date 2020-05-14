package com.wells.flink.demo.api.transform;

import com.wells.flink.demo.entity.Person;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Description Flink Transform算子练习
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
//        transformTest.reduce(env);
//        transformTest.fold(env);
        transformTest.aggregations(env);


        env.execute();
    }

    /**
     * @desc Aggregate 对KeyedStream按指定字段滚动聚合并输出每一次滚动聚合后的结果。默认的聚合函数有:sum、min、minBy、max、mabBy。
     * @method aggregations 聚合算子
     * 注意:
     * 1、max(field)与maxBy(field)的区别: maxBy返回field最大的那条数据; 而max则是将最大的field的值赋值给第一条数据并返回第一条数据。
     * 同理,min与minBy。
     * 2、Aggregate聚合算子会滚动输出每一次聚合后的结果。
     * @param env
     * @return void
     * @date 2020-05-14 20:50:48
     * @author wells
     */
    private void aggregations(StreamExecutionEnvironment env) {
        List<Person> personList = new ArrayList<Person>();
        personList.add(new Person("Allen", 18, "male"));
        personList.add(new Person("Tom", 23, "female"));
        personList.add(new Person("Jerry", 10, "male"));
        personList.add(new Person("Tom", 45, "male"));
        personList.add(new Person("Tom", 33, "male"));

        DataStreamSource<Person> personDataStreamSource = env.fromCollection(personList);

        KeyedStream<Person, String> personStringKeyedStream = personDataStreamSource.keyBy(new KeySelector<Person, String>() {
            @Override
            public String getKey(Person person) throws Exception {
                return person.getName();
            }
        });

        // 滚动年龄求和
        personStringKeyedStream.sum("age").print();
        // 滚动max
        personStringKeyedStream.max("age").print();
        // 滚动maxBy
        personStringKeyedStream.maxBy("age").print();
        // 滚动min
        personStringKeyedStream.min("age").print();
        // 滚动minBy
        personStringKeyedStream.minBy("age").print();

    }

    /**
     * @desc 基于初始值和FoldFunction进行滚动折叠(Fold)，并向下游算子输出每次滚动折叠后的结果。
     * @method fold
     * @param env
     * @return void
     * @date 2020-05-14 20:47:41
     * @author wells
     */
    public void fold(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("/Users/wells/Projects/04-GitHub/java/flink-demo/src/main/resources/wordCountFile.txt");
        source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }).keyBy(0).fold("init", new FoldFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String fold(String initValue, Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0 + "|" + initValue + "|" + tuple2.f1;
            }
        }).print();
    }

    /**
     * @desc 基于ReduceFunction进行滚动聚合，并向下游算子输出每次滚动聚合后的结果
     * 注意：
     * 1、执行 reduce 操作只能是 KeyedStream
     * @method reduce
     * @param env
     * @return void
     * @date 2020-05-14 20:47:14
     * @author wells
     */
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

    /**
     * @desc 按指定的Key对数据重分区，将同一Key的数据放到同一个分区。
     * 注意:
     * 1、分区结果和KeyBy下游算子的并行度强相关。如下游算子只有一个并行度,不管怎么分，都会分到一起。
     * 2、对于POJO类型，KeyBy可以通过keyBy(fieldName)指定字段进行分区。
     * 3、对于Tuple类型，KeyBy可以通过keyBy(fieldPosition)指定字段进行分区。
     * 4、对于一般类型，如上, KeyBy可以通过keyBy(new KeySelector {...})指定字段进行分区。
     * @method keyBy
     * @param env
     * @return void
     * @date 2020-05-14 20:44:44
     * @author wells
     */
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

    /**
     * @desc 过滤
     * @method filter
     * @param env
     * @return void
     * @date 2020-05-14 20:44:31
     * @author wells
     */
    public void filter(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("/Users/wells/Projects/04-GitHub/java/flink-demo/src/main/resources/wordCountFile.txt");
        source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String line) throws Exception {
                return line.toLowerCase().contains("java");
            }
        }).print().setParallelism(1);
    }

    /**
     * @desc 一对多输出
     * @method flatMap
     * @param env
     * @return void
     * @date 2020-05-14 20:44:17
     * @author wells
     */
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

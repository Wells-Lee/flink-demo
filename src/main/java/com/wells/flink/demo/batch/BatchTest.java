package com.wells.flink.demo.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Description Flink批处理
 * Created by wells on 2020-03-17 19:33:50
 */

public class BatchTest {
    public static void main(String[] args) throws Exception {
        if (null == args || args.length != 1) {
            System.out.println("please check param, Usage: filePath");
            return;
        }

        String filePath = args[0];

        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
        AggregateOperator<Tuple2<String, Integer>> counts = env.readTextFile(filePath).flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }).groupBy(0).sum(1);

        counts.print();
    }
}

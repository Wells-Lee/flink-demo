package com.wells.flink.demo.time;

import com.wells.flink.demo.entity.Person;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Description BoundedOutOfOrdernessTimestampExtractor实例
 * Created by wells on 2020-05-24 15:12:52
 */

public class WatermarksTest {
    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 9999;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 在测试时，必须设置，否则可能hash到不同的分区，导致观察不准，线上可不设置
        env.setParallelism(1);

        // 设置使用 eventTime: 这个设置会赋予默认的周期性生成水位线时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置水位线周期性生成时间
        env.getConfig().setAutoWatermarkInterval(100);

        DataStreamSource<String> dataStreamSource = env.socketTextStream(host, port);
        SingleOutputStreamOperator<Person> personStreamSource = dataStreamSource.map(new MapFunction<String, Person>() {
            @Override
            public Person map(String line) throws Exception {
                String[] split = line.split(",", -1);
                return new Person(split[0], Integer.parseInt(split[1]), split[2], Long.parseLong(split[3]));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Person>(Time.seconds(2)) {
            // 提取EventTime和设置延迟时间
            @Override
            public long extractTimestamp(Person element) {
                return element.getTimestamp();
            }
        });

        // 对已经设置过水位线的流进行操作
        personStreamSource
                .map(new MapFunction<Person, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Person person) throws Exception {
                        return new Tuple2<>(person.getName(), person.getAge());
                    }
                })
                .keyBy(0)
                // 注意: 这里不会根据系统时间等待10s后运行，而是提取EventTime跨度大于10s
                .timeWindow(Time.seconds(10))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> res, Tuple2<String, Integer> t1) throws Exception {
                        return new Tuple2<>(res.f0, Math.max(res.f1, t1.f1));
                    }
                })
                .print();

        env.execute();
    }
}
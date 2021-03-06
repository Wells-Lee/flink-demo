package com.wells.flink.demo.time;

import com.wells.flink.demo.entity.Person;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * Description 自定义 TimeAssigner
 * Created by wells on 2020-05-24 15:12:52
 */

public class WatermarkCustomAssignerTest {
    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 9999;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置使用 eventTime: 这个设置会赋予默认的周期性生成水位线时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置水位线周期性生成时间
        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Person> personStreamSource = env.socketTextStream(host, port)
                .map(new MapFunction<String, Person>() {
                    @Override
                    public Person map(String line) throws Exception {
                        String[] split = line.split(",");
                        return new Person(split[0], Integer.parseInt(split[1]), split[2], Long.parseLong(split[3]));
                    }
                })
                // 提取时间戳以及 Watermark
                .assignTimestampsAndWatermarks(new MyPeriodAssigner(1000));
        // 对已经设置过水位线的流进行操作
        personStreamSource
                .map(new MapFunction<Person, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Person person) throws Exception {
                        return new Tuple2<>(person.getName(), person.getAge());
                    }
                })
                .keyBy(0)
//                // 注意: 这里不会根据系统时间等待10s后运行，而是提取EventTime跨度大于10s
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

/**
 * 周期性生成Watermark
 */
class MyPeriodAssigner implements AssignerWithPeriodicWatermarks<Person> {
    long maxTs = Long.MIN_VALUE;
    long bound = 0;

    public MyPeriodAssigner(long bound) {
        this.bound = bound;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
//        System.out.println(maxTs - bound);
        return new Watermark(maxTs - bound);
    }

    @Override
    public long extractTimestamp(Person element, long previousElementTimestamp) {
        maxTs = Math.max(maxTs, element.getTimestamp());
        return element.getTimestamp();
    }
}

/**
 * 无规律生成
 */
class MyPunctuatedAssigner implements AssignerWithPunctuatedWatermarks<Person> {
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(Person lastElement, long extractedTimestamp) {
        // 直接使用提取的时间戳当做水位线，即每条记录都会带有 Watermark；也可以根据某种条件来触发
        return new Watermark(extractedTimestamp);
    }

    @Override
    public long extractTimestamp(Person element, long previousElementTimestamp) {
        return element.getTimestamp();
    }
}
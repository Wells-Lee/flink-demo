package com.wells.flink.demo.process;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Description ProcessAPI
 * Created by wells on 2020-05-24 17:45:41
 */

public class ProcessAPITest {
    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 9999;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream(host, port);
        dataStreamSource.print("socketSource");
        SingleOutputStreamOperator<String> processStreamSource = dataStreamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                String[] split = line.split(" ", -1);
                return new Tuple2<String, Integer>(split[0], Integer.parseInt(split[1]));
            }
        }).keyBy(0).process(new MyKeyedProcessFunction());
        processStreamSource.print("processSource");

        env.execute();
    }
}

class MyKeyedProcessFunction extends KeyedProcessFunction<Tuple, Tuple2<String, Integer>, String> {
    // 定义一个状态，用来保存上一行记录的数字
    private transient ValueState<Integer> lastNumber;

    // 记录增加的定时器的时间，方便清除
    private transient ValueState<Long> currentTimerTs;

    @Override
    public void open(Configuration parameters) throws Exception {
        lastNumber = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastNumber", Integer.class, 0));
        currentTimerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("currentTimerTs", Long.class, 0L));
    }

    // 处理流中的每一个元素
    @Override
    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
        // 得到上一次的状态值
        Integer preNumber = lastNumber.value();

        // 更新上一次的状态值
        lastNumber.update(value.f1);

        // 当前定时器的时间
        Long currTimerTs = currentTimerTs.value();

        // 如果数字减小或者第一次设置定时器，则触发定时器
        if (value.f1 < preNumber && currTimerTs == 0) {
            // 触发定时器时间: 在当前时间基础上加10s 即 当前时间过10s后数字下降，触发定时器
            long timerTs = ctx.timerService().currentProcessingTime() + 10000L;
            ctx.timerService().registerProcessingTimeTimer(timerTs);
            currentTimerTs.update(timerTs);
        } else if (value.f1 > preNumber || preNumber == 0) {
            // 数字变大 或者 第一次进入 则删除定时器
            ctx.timerService().deleteProcessingTimeTimer(currTimerTs);
            currentTimerTs.clear();
        }
    }

    // 定时器要做的事情
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        out.collect("number more than last number");
        currentTimerTs.clear();
    }
}
package com.wells.flink.demo.process;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Description 侧输出流，通过
 * Created by wells on 2020-05-24 19:06:57
 */

public class SideOutputTest {
    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 9999;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream(host, port);
        dataStreamSource.print("socketSource");
        SingleOutputStreamOperator<String> processStreamSource = dataStreamSource.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String line) throws Exception {
                return Integer.parseInt(line);
            }
        }).process(new MyProcessFunction());
        processStreamSource.print("processSource");
        processStreamSource.getSideOutput(new OutputTag<String>("numberDecrease", TypeInformation.of(String.class)))
                .print("numberDecrease");

        env.execute();
    }
}

class MyProcessFunction extends ProcessFunction<Integer, String> {
    private transient OutputTag<String> outputTag;

    @Override
    public void open(Configuration parameters) throws Exception {
        outputTag = new OutputTag<String>("numberDecrease", TypeInformation.of(String.class));
    }

    // 处理流中的每一个元素
    @Override
    public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
        // 如果数字减小或者第一次设置定时器，则触发定时器
        if (value < 0) {
            ctx.output(outputTag, String.valueOf(value));
        } else {
            out.collect(String.valueOf(value));
        }
    }
}
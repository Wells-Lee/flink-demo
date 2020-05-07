package com.wells.flink.demo.source;

import com.wells.flink.demo.entity.Person;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Description 
 * Created by wells on 2020-05-06 20:02:11
 */

public class FromElementsSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Person> personDataStreamSource = env.fromElements(
                new Person("tom", 23, "man"),
                new Person("jerry", 2, "woman")
        );

        personDataStreamSource.print();

        env.execute();
    }
}

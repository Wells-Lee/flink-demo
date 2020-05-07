package com.wells.flink.demo.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * Description Kafka API
 * Created by wells on 2020-03-25 20:36:53
 */

public class KafkaSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "flink-demo-topic-group");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer011<String>("flink-demo-topic", new SimpleStringSchema(), properties));

        stream.print();
        env.execute();
    }
}

package com.wells.flink.demo.api.source;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * Description 可以检测文件内容变化和文件新增, 需要注意: FileProcessingMode模式的区别
 * Created by wells on 2020-05-06 20:14:42
 */

public class FileSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String filePath = "/Users/wells/Temp/data/wordcount";

        DataStreamSource<String> stringDataStreamSource = env.readFile(new TextInputFormat(new Path(filePath)), filePath,
                FileProcessingMode.PROCESS_CONTINUOUSLY, 1000);

        stringDataStreamSource.print();
        env.execute();
    }
}

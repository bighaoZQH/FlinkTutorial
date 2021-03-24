package org.bighao.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bighao.apitest.source.beans.SensorReading;

import java.util.Arrays;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/3/21 0:43
 */
public class SourceTest2_File {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从文件中读取数据
        DataStream<String> dataStream = env.readTextFile("E:\\code\\java_code\\learn_something\\FlinkTutorial\\src\\main\\resources\\senso.txt");

        // 打印输出
        dataStream.print("传感器温度数据");

        env.execute("test-job");
    }

}

package org.bighao.apitest.window;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.bighao.apitest.source.beans.SensorReading;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/4/1 23:32
 */
public class WindowTest1_TimeWindow_OtherAPI {

    /**
     * Flink窗口函数
     *
     * 允许1分钟内的迟到数据<=比如数据产生时间在窗口范围内，但是要处理的时候已经超过窗口时间了
     * .allowedLateness(Time.minutes(1))
     * 什么是迟到的数据？ 引出Flink的时间语义概念
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("192.168.159.15", 7777);


        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        // Flink窗口函数其他可选的API
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };
        SingleOutputStreamOperator<SensorReading> sumStream = dataStream.keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                // .trigger() // 触发器，一般不使用
                // .evictor() // 移除器，一般不使用
                // 允许1分钟内的迟到数据<=比如数据产生时间在窗口范围内，但是要处理的时候已经超过窗口时间了
                .allowedLateness(Time.minutes(1))
                // 侧输出流，迟到超过1分钟的数据，收集于此
                .sideOutputLateData(outputTag)
                // 侧输出流 对 温度信息 求和
                // 之后可以再用别的程序，把侧输出流的信息和前面窗口的信息聚合。（可以把侧输出流理解为用来批处理来补救处理超时数据）
                .sum("temperature");

        sumStream.getSideOutput(outputTag).print("late");

        sumStream.print();

        env.execute();
    }

}

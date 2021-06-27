package org.bighao.apitest.tableapi;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.bighao.apitest.source.beans.SensorReading;

import java.time.Duration;


/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/6/27 22:05
 */
public class TableTest5_TimeAndWindow {


    /**
     * 流转table，设定时间语义
     */
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 读入文件数据，得到DataStream
        DataStream<String> inputStream = env.readTextFile("E:\\code\\java_code\\learn_something\\FlinkTutorial\\src\\main\\resources\\senso.txt");

        // 3. 转换成POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        })/*.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
              @Override
              public long extractTimestamp(SensorReading element) {
                return element.getTimestamp() * 1000;
              }
            });*/
        .assignTimestampsAndWatermarks(
                WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                        @Override
                        public long extractTimestamp(SensorReading element, long l) {
                            return element.getTimestamp() * 1000;
                        }
                    }));

        // 4. 将流转换成表，定义时间特性
        // 处理时间
        /*Table dataTable = tableEnv.fromDataStream(dataStream,
                "id, timestamp as ts, temperature as temp, pt.proctime");*/

        // 事件时间
        Table dataTable = tableEnv.fromDataStream(dataStream,
                "id, timestamp.rowtime as ts, temperature as temp");

        dataTable.printSchema();
        tableEnv.toAppendStream(dataTable, Row.class).print();

        env.execute();
    }

}
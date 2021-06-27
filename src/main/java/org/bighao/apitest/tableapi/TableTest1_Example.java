package org.bighao.apitest.tableapi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.bighao.apitest.source.beans.SensorReading;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/6/26 22:30
 */
public class TableTest1_Example {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.读取数据
        DataStreamSource<String> inputStream = env.readTextFile("E:\\code\\java_code\\learn_something\\FlinkTutorial\\src\\main\\resources\\senso.txt");
        // 2.转换成pojo
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 3.创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 4.基于流，创建一张表
        Table dataTable = tableEnv.fromDataStream(dataStream);

        // 5.调用tableApi进行转换操作
        Table resultTable = dataTable.select("id, temperature")
                .where("id = 'sensor_1'");

        // 6.执行sql
        tableEnv.createTemporaryView("sensor", dataTable);
        String sql = "select id, temperature from sensor where id = 'sensor_1'";
        Table resultSqlTable = tableEnv.sqlQuery(sql);

        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }


}

package org.bighao.apitest.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bighao.apitest.source.beans.SensorReading;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/3/28 19:51
 */
public class SinkTest4_JDBC {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile(System.getProperty("user.dir") + "\\src\\main\\resources\\senso.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        // 定义JDBC的连接配置
        dataStream.addSink(new MyJdbcSink());

        env.execute();
    }

    // 实现自定义的JDKC sinkFunction 通过RichSinkFunction来实现生命周期管理，在生命周期里去创建连接
    private static class MyJdbcSink extends RichSinkFunction<SensorReading> {
        // 声明连接器 和 sql预编译语句
        Connection connection;
        PreparedStatement insertStmt;
        PreparedStatement updateStmt;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://192.168.159.15:3306/flink-test", "root", "123456");
            insertStmt = connection.prepareStatement("insert into sensor_temp(id, temp) values(?, ?)");
            insertStmt = connection.prepareStatement("update sensor_temp set temp = ? where id = ?");
        }

        // 每来一条数据，调用链接，执行sql
        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            updateStmt.setDouble(1, value.getTemperature());
            updateStmt.setString(2, value.getId());
            updateStmt.execute();
            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, value.getId());
                insertStmt.setDouble(2, value.getTemperature());
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }
    }

}

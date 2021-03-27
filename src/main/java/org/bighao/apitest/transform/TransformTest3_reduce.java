package org.bighao.apitest.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bighao.apitest.source.beans.SensorReading;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/3/21 14:55
 */
public class TransformTest3_reduce {

    /**
     * reduce聚合
     * 复杂场景，除了获取最大温度的整个传感器信息以外，还要求时间戳更新成最新的
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile(System.getProperty("user.dir") + "\\src\\main\\resources\\senso.txt");

        // 1.转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        // 2.分组
        KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(SensorReading::getId);
        // reduce聚合，取最大的温度值，以及当前最新的时间戳
        // curState是之前聚合的结果，newState是当前最新传过来的数据
        DataStream<SensorReading> resultStream = keyedStream.reduce((curState, newState) -> {
            return new SensorReading(curState.getId(),
                    newState.getTimestamp(),
                    Math.max(curState.getTemperature(), newState.getTemperature()));
        });
        resultStream.print("result");
        env.execute();
    }

}

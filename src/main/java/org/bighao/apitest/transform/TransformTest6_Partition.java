package org.bighao.apitest.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bighao.apitest.source.beans.SensorReading;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/3/27 23:04
 * <p>
 * Flink重分区操作
 */
public class TransformTest6_Partition {


    /**
     * Flink重分区操作
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<String> inputStream = env.readTextFile(System.getProperty("user.dir") + "\\src\\main\\resources\\senso.txt");

        // SingleOutputStreamOperator多并行度默认就rebalance,轮询方式分配
        inputStream.print("input");

        // 1.shuffle 洗牌 - 随机(并非批处理中的获取一批后才打乱，这里每次获取到直接打乱且分区)
        inputStream.shuffle().print("shuffle");

        // 2.keyBy 基于key的hashCode取模后进行重分区，这样key相同的都会在一个分区中，当然由于hash冲突，不同的key就有可能在一个分区中来处理
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });
        dataStream.keyBy("id").print("keyBy");

        // 3.global 直接将所有数据全部发送到下游的第一个分区中，相当于没有了并行，少数特殊情况才用
        inputStream.global().print("global");

        env.execute();
    }

}

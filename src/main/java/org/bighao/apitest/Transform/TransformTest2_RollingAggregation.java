package org.bighao.apitest.Transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bighao.apitest.source.beans.SensorReading;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/3/21 14:55
 */
public class TransformTest2_RollingAggregation {

    /**
     * 滚动聚合
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("E:\\code\\java_code\\learn_something\\FlinkTutorial\\src\\main\\resources\\senso.txt");

        // 1.转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        // 2.分组
        //dataStream.keyBy("id"); 过期了
        KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(item -> item.getId());

        // 3.滚动聚合，取当前最大的温度值
        // min():获取的最小值，指定的field是最小，但不是最小的那条记录，后面的示例会清晰的显示。
        // minBy():获取的最小值，同时也是最小值的那条记录。
        // min 和 minBy 都会返回整个元素，只是 min 会根据用户指定的字段取最小值，
        // 并且把这个值保存在对应的位置，而对于其他的字段，并不能保证其数值正确。max 和 maxBy 同理。
        SingleOutputStreamOperator<SensorReading> resultMax = keyedStream.max("temperature");
        SingleOutputStreamOperator<SensorReading> resultMaxBy = keyedStream.maxBy("temperature");

        resultMax.print("max");
        //resultMaxBy.print("maxby");

        env.execute();
    }

}

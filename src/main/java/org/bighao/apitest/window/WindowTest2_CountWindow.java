package org.bighao.apitest.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bighao.apitest.source.beans.SensorReading;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/4/1 23:32
 */
public class WindowTest2_CountWindow {

    /**
     * Flink窗口函数
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("192.168.159.15", 7777);


        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        // 计数窗口测试 统计几个传感器探测各自的温度平均值
        SingleOutputStreamOperator<Double> avgTempResultStream = dataStream.keyBy(SensorReading::getId)
                // 10个数据开一个窗口，隔2个统计一次
                .countWindow(10, 2)
                .aggregate(new MyAvgTemp());


        avgTempResultStream.print();

        env.execute();
    }

    private static class MyAvgTemp implements AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double> {
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0d, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {
            return new Tuple2<>(accumulator.f0 + value.getTemperature(), accumulator.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> acc1, Tuple2<Double, Integer> acc2) {
            return new Tuple2<>(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
        }
    }
}

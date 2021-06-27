package org.bighao.apitest.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.bighao.apitest.source.beans.SensorReading;
import scala.Tuple3;

import java.util.Map;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/5/30 23:00
 */
public class StateTest3_KeyedStateApplicationCase {


    /**
     * 键控状态 - 案例
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("192.168.159.15", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        // 定义一个flatMap操作，检测温度跳变输出报警
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = dataStream.keyBy(SensorReading::getId)
                .flatMap(new TempChangeWarning(10.0d));

        resultStream.print();
        env.execute();
    }

    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {
        // 当前温度跳变阈值
        private Double threshold;

        public TempChangeWarning(Double threshold) {
            this.threshold = threshold;
        }

        // 定义状态，保存上一次的温度值
        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
        }

        @Override
        public void flatMap(SensorReading in, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            // 获取上一次的温度值（即上一次的状态）
            Double lastTemp = lastTempState.value();
            // 如果状态不为null，那么就判断两次温度差值是否大于设定的阈值
            if (lastTemp != null) {
                Double diff = Math.abs(in.getTemperature() - lastTemp);
                if (diff >= threshold) {
                    out.collect(new Tuple3<>(in.getId(), lastTemp, in.getTemperature()));
                }
            }
            // 更新状态
            lastTempState.update(in.getTemperature());
        }

        @Override
        public void close() throws Exception {
            // 清理状态
            lastTempState.clear();
        }
    }

}

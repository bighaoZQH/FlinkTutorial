package org.bighao.apitest.processfunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.bighao.apitest.source.beans.SensorReading;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/6/6 0:50
 */
public class ProcessTest1_KeyedProcessFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("192.168.159.15", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        // 测试KeyedProcessFunction，先分组然后自定义处理
        dataStream.keyBy(SensorReading::getId)
                .process(new MyProcess())
                .print();

        env.execute();
    }

    // 实现自定义的处理函数 <key, in, out> key是keyBy的key类型，比如keyBy("id")得到的是Tuple类型
    public static class MyProcess extends KeyedProcessFunction</*Tuple*/String, SensorReading, Integer> {

        private ValueState<Long> tsTimerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<>("ts-timer", Long.class));
        }

        @Override
        public void processElement(SensorReading in, Context ctx, Collector<Integer> out) throws Exception {
            out.collect(in.getId().length());

            // context的作用？
            ctx.timestamp();
            ctx.getCurrentKey();
            // 侧输出流..
            //ctx.output();
            // 定时服务
            ctx.timerService().currentProcessingTime();
            ctx.timerService().currentWatermark();

            // 定时器
            // 在当前处理时间的5秒延迟后触发
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
            tsTimerState.update(ctx.timerService().currentProcessingTime() + 5000L);
            //ctx.timerService().registerEventTimeTimer((in.getTimestamp() + 10) * 1000);
            // 删除定时器，根据时间来判断删哪个
            //ctx.timerService().deleteEventTimeTimer(tsTimerState.value());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp + "定时器触发");
            ctx.getCurrentKey();
            // 测输出流
            //ctx.output();
            // 输出到主流
            //out.collect();
            // 判断当前是处理时间，还是事件时间
            TimeDomain timeDomain = ctx.timeDomain();
        }

        @Override
        public void close() throws Exception {
            tsTimerState.clear();
        }
    }


}

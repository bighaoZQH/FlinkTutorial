package org.bighao.apitest.processfunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.bighao.apitest.source.beans.SensorReading;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/6/6 12:32
 */
public class ProcessTest2_ApplicationCase {

    /**
     * sensor_1,1547718207,36
     * sensor_1,1547718217,37
     * sensor_1,1547718227,38
     * sensor_1,1547718237,39
     * sensor_1,1547718247,40 --等10秒输出结果
     *
     * sensor_1,1547718247,25 -- 这样的话是不输出的，因为不是连续上升的
     *
     */
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
                .process(new TempConsIncreWarning(10000L))
                .print();

        env.execute();
    }

    // 实现自定义处理函数，检测一段时间内的温度连续上升，输出报警
    public static class TempConsIncreWarning extends KeyedProcessFunction<String, SensorReading, String> {

        // 报警的时间间隔(如果在interval时间内温度持续上升，则报警) ms
        private Long interval;

        public TempConsIncreWarning(Long interval) {
            this.interval = interval;
        }

        // 定义状态，上一个温度值
        private ValueState<Double> lastTempState;
        // 最近一次定时器的触发时间(报警时间)
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<>("last-temp", Double.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(SensorReading in, Context ctx, Collector<String> out) throws Exception {
            // 当前温度值
            Double curTemp = in.getTemperature();
            // 上一次温度(没有则设置为一个最小温度值)
            Double lastTemp = lastTempState.value() != null ? lastTempState.value() : Double.MIN_VALUE;
            // 计时器状态值(时间戳)
            Long timerTs = timerTsState.value();

            // 如果温度上升 且 当前没有定时器的时候，注册定时器，开始等待
            if (curTemp > lastTemp && timerTs == null) {
                // 计算出定时器时间戳
                long ts = ctx.timerService().currentProcessingTime() + interval;
                // 注册定时器
                ctx.timerService().registerProcessingTimeTimer(ts);
                // 更新定时器状态
                timerTsState.update(ts);
            }
            // 如果温度下降，那么删除定时器
            else if (curTemp < lastTemp && timerTs != null) {
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                // 清空保存的定时器状态
                timerTsState.clear();
            }

            // 更新最新的温度状态
            lastTempState.update(curTemp);
            out.collect("aa");
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，输出报警信息
            out.collect("传感器" + ctx.getCurrentKey() + "温度值连续" + interval + "秒上升, 最高温度值" + lastTempState.value());
            // 清空保存的定时器状态
            timerTsState.clear();
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }

}

package org.bighao.apitest.window;

import akka.japi.tuple.Tuple3;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.bighao.apitest.source.beans.SensorReading;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/4/1 23:32
 */
public class WindowTest1_TimeWindow_FullWindow {

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

        // 全窗口函数 特点即数据过来先不处理，等到窗口临界再遍历、计算、输出结果。
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> resultStream = dataStream.keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                // process 也可以实现全窗口函数
                // .process(new ProcessWindowFunction<SensorReading, Object, Tuple, TimeWindow>() {
                // WindowFunction<IN, OUT, KEY, W extends Window> key就是分组的key，w是窗口类型
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow timeWindow, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        long windowEnd = timeWindow.getEnd();
                        int count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<>(key, windowEnd, count));
                    }
                });

        resultStream.print();

        env.execute();
    }

}

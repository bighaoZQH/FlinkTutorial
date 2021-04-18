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
public class WindowTest1_TimeWindow_Aggregate {

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

        // 开窗测试 - 增量聚合 特点即每次数据过来都处理，但是**到了窗口临界才输出结果
        DataStream<Integer> resultStream = dataStream.keyBy(SensorReading::getId)
                // .countWindow(10, 2);
                // .window(EventTimeSessionWindows.withGap(Time.minutes(1)));
                // .timeWindow(Time.seconds(15)) // 已经不建议使用@Deprecated
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                // 增量聚合 AggregateFunction<IN, ACC, OUT> in就是输入，out就是输出，acc是中间值-累加器
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {

                    /**
                     * 创建累加器
                     * @return 累加器的初始值
                     */
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    /**
                     * 累加器进行累加
                     * @param sensorReading 传入的值
                     * @param accumulater   累加器
                     * @return
                     */
                    @Override
                    public Integer add(SensorReading sensorReading, Integer accumulater) {
                        return accumulater + 1;
                    }


                    /**
                     * 获取累加结果
                     * @param accumulater
                     * @return
                     */
                    @Override
                    public Integer getResult(Integer accumulater) {
                        return accumulater;
                    }

                    /**
                     * 合并累加器
                     * @param acc1
                     * @param acc2
                     * @return
                     */
                    @Override
                    public Integer merge(Integer acc1, Integer acc2) {
                        return acc1 + acc2;
                    }
                });

        resultStream.print();

        env.execute();
    }

}

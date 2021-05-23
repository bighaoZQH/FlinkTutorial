package org.bighao.apitest.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.bighao.apitest.source.beans.SensorReading;

import java.time.Duration;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/4/11 23:20
 */
public class WindowTest3_EventTimeWindow {


    /**
     * 具体看MD文档
     * Flink对于迟到数据有三层保障，先来后到的保障顺序是：
     * - WaterMark => 约等于放宽窗口标准
     * - allowedLateness => 允许迟到（ProcessingTime超时，但是EventTime没超时）
     * - sideOutputLateData => 超过迟到时间，另外捕获，之后可以自己批处理合并先前的数据
     *
     * 设置了15秒的滑动窗口，
     * 延时3秒的waterMark，（也就是说，这个waterMark设置了3秒后，相当于时间还处在cur - 3秒的时候，因此不会触发窗口关闭）
     * 因此案例中的起始窗口时间范围就是[195-210)，当延时3秒后的数据213到达后，会关闭[195-210)的窗口
     * 注意是不包括210的，210的数据落在了[210-225)的窗口中
     *
     * 测试数据
     *
     * sensor_6,1547718201,15.4
     * sensor_7,1547718202,6.7
     * sensor_10,1547718205,38.1
     *
     * sensor_1,1547718199,39.8
     * sensor_1,1547718207,36.3
     * sensor_1,1547718210,21.1
     * sensor_1,1547718209,32.8
     *
     * sensor_1,1547718211,20.11
     * sensor_1,1547718212,30.23
     * sensor_1,1547718206,11.8   这个206虽然迟到了,但是还在waterMarker之内，因此也会进入窗口中，进行聚合
     * sensor_1,1547718213,11.23  ===>waterMark3秒的节点
     * sensor_1,1547718205,10.5   这个205在waterMarker之后，窗口已经关了，就无法聚合计算，那结果就不准了呀，怎么办呢? allowedLateness，允许迟到的数据的时间
     *
     * sensor_1,1547718214,19.23
     * sensor_1,1547718215,32.23
     * sensor_1,1547718224,8.23
     * sensor_1,1547718225,7.23
     *
     * sensor_1,1547718227,7.23
     * sensor_1,1547718228,2.23
     *
     * // 1分钟多后。。。
     * sensor_1,1547718328,2.23
     * sensor_1,1547718204,1.8 那这个204也迟到了，而且超过了allowedLateness的范围，怎么办呢？sideOutputLateData测输出流来进行聚合
     *
     * 结果：
     * minTemp> SensorReading(id=sensor_1, timestamp=1547718206, temperature=11.8)
     * minTemp> SensorReading(id=sensor_1, timestamp=1547718205, temperature=10.5)
     * minTemp> SensorReading(id=sensor_1, timestamp=1547718224, temperature=8.23)
     * lateTemp> SensorReading(id=sensor_1, timestamp=1547718204, temperature=1.8)
     *
     * 因此看到flink提供了3层保障，来处理迟到数据
     *
     * MyAssigner有两种类型
     * - AssignerWithPeriodicWatermarks
     * - AssignerWithPunctuatedWatermarks
     *
     * 以上两个接口都继承自TimestampAssigner。
     *
     * [AssignerWithPeriodicWatermarks]
     * - 周期性的生成 watermark：系统会周期性的将 watermark 插入到流中
     * - 默认周期是200毫秒，可以使用 `ExecutionConfig.setAutoWatermarkInterval()` 方法进行设置
     * - 升序和前面乱序的处理 BoundedOutOfOrderness ，都是基于周期性 watermark 的。
     *
     * [AssignerWithPunctuatedWatermarks]
     * - 没有时间周期规律，可打断的生成 watermark（即可实现每次获取数据都更新watermark）
     *
     * [Watermark的设定]
     * - 在Flink中，Watermark由应用程序开发人员生成，这通常需要对相应的领域有一定的了解
     * - 如果Watermark设置的延迟太久，收到结果的速度可能就会很慢，解决办法是在水位线到达之前输出一个近似结果
     * - 如果Watermark到达得太早，则可能收到错误结果，不过Flink处理迟到数据的机制可以解决这个问题
     *
     * 一般大数据场景都是考虑高并发情况，所以一般使用周期性生成Watermark的方式，避免频繁地生成Watermark。
     *
     * 也就是说：数据很频繁的情况下，每次都更新最新的Watermark造成开销大，且会肯定会有重复更新相同的waterMark的情况，因此周期性的会更好。
     * 但数据很稀疏的情况下，如果空档期多且时间长，那么周期性的waterMark也会去更新，但没有用，因此更适用获取数据都更新去watermark。
     *
     * flink1.12.x 设置waterMark https://zhuanlan.zhihu.com/p/345655175
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从调用时刻开始给env创建的每一个stream追加时间特征 - 事件时间
        // 在Flink的流式处理中，绝大部分的业务都会使用eventTime，一般只在eventTime无法使用时，才会被迫使用ProcessingTime或者IngestionTime。
        // Flink1.12.X 已经默认就是使用EventTime了，所以不需要这行代码
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置周期性waterMark的时间
        env.getConfig().setAutoWatermarkInterval(100);

        // 转换成SensorReading类型，分配时间戳和waterMark
        DataStreamSource<String> inputStream = env.socketTextStream("192.168.159.15", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        })
                // 升序数据设置事件时间和waterMark
                // 旧版 (新版官方推荐用assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps) )
                /*.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
                    @Override
                    public long extractAscendingTimestamp(SensorReading element) {
                        return element.getTimestamp();
                    }
                })*/
                // 乱序数据设置时间戳和waterMark
                // 旧版 (新版官方推荐用assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness) )
                /*.assignTimestampsAndWatermarks(
                        // 有界乱序情况下的一个时间戳提取器，参数是给一个延时时间来设定warterMark
                        new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(3)) {
                            @Override
                            public long extractTimestamp(SensorReading element) {
                                // 要求返回毫秒级别的时间戳 所以*1000
                                return element.getTimestamp() * 1000L;
                            }
                        });*/
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new MyTimeAssigner()));

        OutputTag<SensorReading> lateOutPutTag = new OutputTag<SensorReading>("lateTag") {
        };

        // 基于事件时间的开窗聚合，统计15秒内温度的最小值
        // TumblingEventTimeWindows事件时间的滚动窗口
        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy(SensorReading::getId)
                //.timeWindow(Time.seconds(15))
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                // 设置延时时间
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateOutPutTag)
                .minBy("temperature");

        minTempStream.print("minTemp");
        minTempStream.getSideOutput(lateOutPutTag).print("lateTemp");

        env.execute();
    }

}


class MyTimeAssigner implements SerializableTimestampAssigner<SensorReading> {

    // 如果是map接口，可以通过构造方法传入时间的字段名，来get(timestampColumn)
    private String timestampColumn;

    public MyTimeAssigner() {}

    public MyTimeAssigner(String timestampColumn) {
        this.timestampColumn = timestampColumn;
    }

    /**
     * 提取数据里的timestamp字段为时间戳
     *
     * @param element         event
     * @param recordTimestamp element 的当前内部时间戳，或者如果没有分配时间戳，则是一个负数
     * @return The new timestamp.
     */
    @Override
    public long extractTimestamp(SensorReading element, long recordTimestamp) {
        return element.getTimestamp() * 1000L;
    }
}

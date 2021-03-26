package org.bighao.apitest.Transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.bighao.apitest.source.beans.SensorReading;
import org.junit.Test;

import java.util.Collections;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/3/21 14:55
 * <p>
 * 两种分流器Spilt-Select和Side-Outputs 侧输出流
 * flink的分流操作分为两种，split已经过时，甚至在1.12版本中已经没有该方法
 * 通过Side-Outputs来代替split，split不能二次分流，而side-outputs可以
 * https://www.jianshu.com/p/74e4e7a51247 我是看这个案例写的代码
 * http://www.wld5.com/javajc/16419.html
 * <p>
 * 两种分流器原理实现
 * https://blog.csdn.net/weixin_41197407/article/details/114022197
 *
 * 分流 与 合流
 * connect合流 同时只能进行两条流的合并 可以两条流的数据类型可以不同，合并时可以转换
 * union合流 可以合并多条流，但这些流的数据类型需要相同
 */
public class TransformTest4_MultipleStreams {

    /**
     * Side-Outputs
     * 测试场景：根据传感器温度高低，划分成两组，high和low（>30归入high）：
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile(System.getProperty("user.dir") + "\\src\\main\\resources\\senso.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        // ================ 1.分流操作，按照温度值30度为界分为两条流 ==============
        // 先定义OutputTag
        OutputTag<SensorReading> less30Tag = new OutputTag<SensorReading>("temperature <= 30") {
        };
        OutputTag<SensorReading> greater30Tag = new OutputTag<SensorReading>("temperature > 30") {
        };

        // 1级分流处理
        SingleOutputStreamOperator<SensorReading> split1Stream = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {
                if (sensorReading.getTemperature() <= 30.0D) {
                    context.output(less30Tag, sensorReading);
                } else {
                    context.output(greater30Tag, sensorReading);
                }
            }
        });

        DataStream<SensorReading> less30Stream = split1Stream.getSideOutput(less30Tag);
        DataStream<SensorReading> greater30Stream = split1Stream.getSideOutput(greater30Tag);

        // 对less30Stream进行2级分流处理
        OutputTag<SensorReading> less10Tag = new OutputTag<SensorReading>("temperature <= 10") {
        };
        OutputTag<SensorReading> greater10Tag = new OutputTag<SensorReading>("10 < temperature <= 30") {
        };
        SingleOutputStreamOperator<SensorReading> split2Stream = less30Stream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {
                if (sensorReading.getTemperature() <= 10.0D) {
                    context.output(less10Tag, sensorReading);
                } else {
                    context.output(greater10Tag, sensorReading);
                }
            }
        });

        DataStream<SensorReading> less10Stream = split2Stream.getSideOutput(less10Tag);
        DataStream<SensorReading> greater10Stream = split2Stream.getSideOutput(greater10Tag);

        greater30Stream.print("一级分流 - 大于30度");
        less10Stream.print("二级分流 - 小于10度");
        greater10Stream.print("二级分流 - 大于10度,小于30度");


        // =============== 2.合流 connect ===============
        // 将高温流转换成二元组类型，与低温流 连接合并后，输出状态信息(超过30度进行告警)
        DataStream<Tuple2<String, Double>> warningStream = greater30Stream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(), sensorReading.getTemperature());
            }
        });

        // ConnectedStreams<IN1, IN2> IN1 - 谁调用，谁就是input1，被调用的是in2
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warningStream.connect(less30Stream);
        // 三个参数 -> 流1类型，流2类型，合并后的类型（注意两个流返回的类型可以不相同，但都需要是第三个参数的类型）
        SingleOutputStreamOperator<Object> resultStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            // 处理流1
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "high warning");
            }

            // 处理流2
            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), "normal");
            }
        });
        resultStream.print("resultStream");

        // ============= 3.合流 union ============
        DataStream<SensorReading> unionStream = less10Stream.union(greater10Stream, greater30Stream);
        unionStream.print("unionStream");

        env.execute();
    }


    /**
     * split-select 已经过时
     */
    @Test
    public void test01 () throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile(System.getProperty("user.dir") + "\\src\\main\\resources\\senso.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                if (sensorReading.getTemperature() <= 30.0D) {
                    return Collections.singleton("low");
                } else {
                    return Collections.singleton("high");
                }
            }
        });

        DataStream<SensorReading> highStream = splitStream.select("high");
        DataStream<SensorReading> lowStream = splitStream.select("low");
        DataStream<SensorReading> allStream = splitStream.select("high", "low");

        highStream.print("high");
        lowStream.print("low");
        allStream.print("all");
        env.execute();
    }

}

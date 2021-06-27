package org.bighao.apitest.processfunction;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.bighao.apitest.source.beans.SensorReading;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/6/6 13:24
 */
public class ProcessTest3_SideOutCase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("192.168.159.15", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        // 定义一个OutPutTag，用来表示侧输出流 低温流
        // An OutputTag must always be an anonymous inner class
        // so that Flink can derive a TypeInformation for the generic type parameter.
        OutputTag<SensorReading> lowTempTag = new OutputTag<SensorReading>("lowTemp") {
        };

        // 测试ProcessFunction，自定义侧输出流，实现分流操作
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading in, Context ctx, Collector<SensorReading> out) throws Exception {
                // 判断温度，大于30度，高温流输出到主流；小于低温流输出到侧输出流
                if (in.getTemperature() > 30) {
                    out.collect(in);
                } else {
                    ctx.output(lowTempTag, in);
                }
            }
        });

        highTempStream.print("high-temp");
        highTempStream.getSideOutput(lowTempTag).print("low-temp");

        env.execute();
    }

}

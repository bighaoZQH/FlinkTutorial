package org.bighao.apitest.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bighao.apitest.source.beans.SensorReading;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/3/27 23:04
 * <p>
 * 富函数
 */
public class TransformTest5_RichFunction {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 调大并行度
        env.setParallelism(4);

        DataStream<String> inputStream = env.readTextFile(System.getProperty("user.dir") + "\\src\\main\\resources\\senso.txt");

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        DataStream<Tuple2<String, Integer>> resultStream = dataStream.map(new MyMapper());

        resultStream.print();

        env.execute();
    }

    // 普通自定义函数UDF 传统的Function不能获取上下文信息，只能处理当前数据，不能和其他数据交互
    private static class MyMapper0 implements MapFunction<SensorReading, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            // 只能获取要处理的数据，而获取不到相关环境信息
            return new Tuple2<>(value.getId(), value.getId().length());
        }
    }

    // 自定义富函数
    private static class MyMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            // 富函数可以获取到相关环境信息  getIndexOfThisSubtask()==>获取当前子任务的编号
            return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        /**
         * 生命周期方法 - 初始化工作，一般是定义状态，或者建立数据库连接等
         * MyMapper这个类初始化的时候会来执行
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println(getRuntimeContext().getIndexOfThisSubtask() + " open");
        }

        /**
         * 生命周期方法 - 关闭链接，清理状态等等收尾操作
         * MyMapper关闭的时候会来调用
         *
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            System.out.println(getRuntimeContext().getIndexOfThisSubtask() + " close");
        }
    }

}

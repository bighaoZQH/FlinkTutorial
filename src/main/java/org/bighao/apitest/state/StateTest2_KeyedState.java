package org.bighao.apitest.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bighao.apitest.source.beans.SensorReading;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/5/30 2:20
 */
public class StateTest2_KeyedState {

    /**
     * 键控状态
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("192.168.159.15", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        // 定义一个有状态的map操作，统计当前sensor数据个数
        SingleOutputStreamOperator<Integer> resultStream = dataStream
                .keyBy(SensorReading::getId)
                .map(new MyKeyCountMapper());

        resultStream.print();
        env.execute();
    }

    // 自定义RichMapFunction
    public static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer> {

        // ValueState
        private ValueState<Integer> keyCountState;
        // 其他类型状态的声明
        private ListState<String> myListState;
        private MapState<String, Double> myMapState;
        private ReducingState<SensorReading> myReducingState;


        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState =
                    // 得到运行上下文
                    getRuntimeContext()
                    // 得到状态的句柄，传入一个状态描述器
                    .getState(new ValueStateDescriptor<Integer>("key-count", Integer.class));

            myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list", String.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));
            //myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>());
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            // 其他状态的api调用
            // listState
            for (String s : myListState.get()) {
                System.out.println(s);
            }
            // list的update是更新整个list，可以选择用add进行追加操作
            //myListState.update();
            myListState.add("hello");

            // mapState
            myMapState.get("1");
            myMapState.put("2", 12.3d);

            // reducing ReducingState是通过声明的ReduceFunction来进行聚合的
            // myReducingState.add();

            // 所有的状态都可以调用clear方法
            myMapState.clear();

            Integer count = keyCountState.value();
            count = count == null ? 0 : count;
            count++;
            // 更新
            keyCountState.update(count);
            return count;
        }

    }

}

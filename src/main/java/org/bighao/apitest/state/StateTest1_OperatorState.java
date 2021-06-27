package org.bighao.apitest.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bighao.apitest.source.beans.SensorReading;

import java.util.Collections;
import java.util.List;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/5/29 23:31
 */
public class StateTest1_OperatorState {

    /**
     * Flink的状态管理 - 算子状态 Operator State
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("192.168.159.15", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        // 定义一个有状态的map操作，统计当前分区数据个数
        SingleOutputStreamOperator<Integer> resultStream = dataStream.map(new MyCountMapper());

        resultStream.print();
        env.execute();
    }


    // 自定义MapFunction<输入类型, 输出类型>
    public static class MyCountMapper implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer> {

        // 定义一个本地变量，作为算子状态，但是这样做有问题，这个变量在内存里啊，机器宕机了就没了，因此需要引入checkpoint
        private Integer count = 0;

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            return ++count;
        }

        /**
         * 对状态进行快照
         */
        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        /**
         * 还原状态
         */
        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer num : state) {
                count += num;
            }
        }
    }

}

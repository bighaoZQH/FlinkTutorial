package org.bighao.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/3/20 16:35
 */
public class StreamWordCount {

    /**
     * 流处理 wordCount
     * <p>
     * 这里env.execute();之前的代码，可以理解为是在定义任务，
     * 只有执行env.execute()后，Flink才把前面的代码片段当作一个任务整体（每个线程根据这个任务操作，并行处理流数据）。
     */
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置全局并行度，也可以每个步骤单独设置
        //env.setParallelism(8);
        // 全局禁用任务链操作
        //env.disableOperatorChaining();

        // socket文本流读取数据
        ///DataStream<String> inputDataStream = env.socketTextStream("192.168.159.15", 7777);
        // 用Parameter Tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        // 基于数据流，进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper())
                // 设置slot共享组，子任务不能共享不同组的slot，如果没有设置共享组，当前步骤默认使用上一个步骤的slot共享组，最初步骤默认会有一个default共享组
                .slotSharingGroup("green")
                // 按照当前key的hashcode进行重分区
                .keyBy(item -> item.f0)
                .sum(1).setParallelism(2)
                // 将当前步骤禁止进行合并任务链(前后都断)
                //.disableChaining()
                // 将当前步骤以一个新的任务链开始执行（即和前边任务链断开）
                //.startNewChain()
                .slotSharingGroup("red");

        resultStream.print().setParallelism(1);

        // 上面的步骤是在 定义 流处理 的过程，接来下就要启动执行任务
        env.execute();
    }

}

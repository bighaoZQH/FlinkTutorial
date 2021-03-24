package org.bighao.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/3/20 16:08
 */
public class WordCount {

    /**
     * 批处理WordCount示例
     */
    public static void main(String[] args) throws Exception {
        // 创建批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        String inputPath = "E:\\code\\java_code\\learn_something\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        // 对数据集进行处理，按空格进行展开 ，转换成(word, 1)二元组进行统计
        DataSet<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapper())
                // 按照第一个位置的word进行分组 new Tuple2<>(word, 1)
                .groupBy(0)
                // 将第二个位置上的数据求和
                .sum(1);
        resultSet.print();
    }

    // 自定义类，实现FlatMapFunction<T, O>接口，T表示输入数据的类型，O表示输出数据的类型
    // Tuple2<String, Integer>表示是二元组 String是word的类型，Integer是统计的结果类型
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 按空格分词
            String[] words = value.split(" ");
            // 遍所有word，包成二元组输出
            for (String word : words) {
                // 收集结果，一个单词，统计一次
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }

}

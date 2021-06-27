package org.bighao.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/6/27 1:26
 */
public class TableTest2_CommonApi_2 {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 表的创建：连接外部系统，读取数据
        // 2.1 读取文件
        String filePath ="E:\\code\\java_code\\learn_something\\FlinkTutorial\\src\\main\\resources\\senso.txt";

        tableEnv.connect(new FileSystem().path(filePath)) // 定义到文件系统的连接
                .withFormat(new Csv()) // 定义以csv格式进行数据格式化
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                ) // 定义表结构
                .createTemporaryTable("inputTable"); // 创建临时表

        Table inputTable = tableEnv.from("inputTable");
        // 打印表结构
        inputTable.printSchema();
        // 将表数据转换成流打印输出
        //tableEnv.toAppendStream(inputTable, Row.class).print();

        // 3.查询转换
        // 3.1 Table API
        // 简单转换
        Table resultTable = inputTable.select("id, temp")
                //.filter("id = 'sensor_6'");
                .filter("id === 'sensor_6'");

        // 聚合统计
        Table aggTable = inputTable.groupBy("id")
                .select("id, id.count as count, temp.avg as avgTemp");

        // 3.2 SQL
        Table sqlResultTable = tableEnv.sqlQuery("select id, temp from inputTable where id = 'sensor_6'");
        Table sqlAggTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");

        // 打印输出 toAppendStream追加 toRetractStream撤回(先删除原本的数据，再添加新数据)
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        // 里面的false表示上一条保存的记录被删除，true则是新加入的数据
        // 所以Flink的Table API在更新数据时，实际是先删除原本的数据，再添加新数据。
        tableEnv.toRetractStream(aggTable, Row.class).print("agg");
        tableEnv.toRetractStream(sqlAggTable, Row.class).print("sqlAgg");

        env.execute();
    }

}

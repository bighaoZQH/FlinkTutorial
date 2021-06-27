package org.bighao.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sinks.AppendStreamTableSink;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/6/27 16:59
 */
public class TableTest3_FileOutput {

    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 表的创建：连接外部系统，读取数据
        // 读取文件
        String filePath ="E:\\code\\java_code\\learn_something\\FlinkTutorial\\src\\main\\resources\\senso.txt";
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");

        Table inputTable = tableEnv.from("inputTable");

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

        // 4. 输出到文件
        // 连接外部文件注册输出表
        String outputPath ="E:\\code\\java_code\\learn_something\\FlinkTutorial\\src\\main\\resources\\senso_out.txt";
        tableEnv.connect(new FileSystem().path(outputPath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        // 配合 aggTable.insertInto("outputTable"); 才使用下面这条
                        // .field("cnt", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");

        // 这条会报错(文件系统输出，不支持随机写，只支持附加写)
        // Exception in thread "main" org.apache.flink.table.api.TableException:
        // AppendStreamTableSink doesn't support consuming update changes which is produced by
        // node GroupAggregate(groupBy=[id], select=[id, COUNT(id) AS EXPR$0, AVG(temp) AS EXPR$1])

        // aggTable.insertInto("outputTable");


        // 旧版可以用下面这条
        // env.execute();

        // 新版需要用这条，上面那条会报错，报错如下
        // Exception in thread "main" java.lang.IllegalStateException:
        // No operators defined in streaming topology. Cannot execute.
        tableEnv.execute("");
    }

}

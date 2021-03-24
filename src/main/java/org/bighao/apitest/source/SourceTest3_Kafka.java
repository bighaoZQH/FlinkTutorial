package org.bighao.apitest.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/3/21 0:43
 */
public class SourceTest3_Kafka {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // 下面这些次要参数
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        // 从kafka中读取数据 需要添加flink连接kafka的依赖，通过addSource()添加数据源
        // new FlinkKafkaConsumer ==> String topic, DeserializationSchema<T> valueDeserializer, Properties props
        // 1.topic 消费的主题 2.value的反序列化工具 3.参数
        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties));

        // 打印输出
        dataStream.print("传感器温度数据");

        env.execute("test-job");
    }

}

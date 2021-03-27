package org.bighao.apitest.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.bighao.apitest.source.beans.SensorReading;

import java.util.Properties;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/3/27 23:46
 */
public class SinkTest1_Kafka {

    /**
     * 往kafka发送数据
     * 这个场景类似做了一个ETL
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        // 1.从kafka中读取数据
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties));

        // 2.对kafka发送来的数据进行处理 - 转换成SensorReading类型
        DataStream<String> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2])).toString();
        });

        // 3.将flink处理后的数据向kafka发送数据
        DataStreamSink<String> sinktest = dataStream.addSink(new FlinkKafkaProducer<String>("localhost:9092",
                "sinktest",
                new SimpleStringSchema()));

        env.execute();
    }

}

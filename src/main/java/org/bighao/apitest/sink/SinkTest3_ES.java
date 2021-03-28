package org.bighao.apitest.sink;

import jdk.nashorn.internal.ir.RuntimeNode;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.http.HttpHost;
import org.bighao.apitest.source.beans.SensorReading;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/3/28 1:33
 */
public class SinkTest3_ES {

    /**
     * 往redis发送数据
     * <p>
     * 模拟将各个传感器的最新温度放入redis缓存
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile(System.getProperty("user.dir") + "\\src\\main\\resources\\senso.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        // 定义ES的连接配置
        List<HttpHost> httpHostList = new ArrayList<>(1);
        httpHostList.add(new HttpHost("192.168.159.15", 9200));
        dataStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHostList, new MyEsSinkFunction()).build());

        env.execute();
    }

    // 自定义的ES写入操作的ElasticsearchSinkFunction
    private static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading> {
        @Override
        public void process(SensorReading element, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            // 定义写入的数据source
            Map<String, String> dataSource = new HashMap<>();
            dataSource.put("id", element.getId());
            dataSource.put("temp", element.getTemperature().toString());
            dataSource.put("ts", element.getTimestamp().toString());

            // 创建请求，作为向es发起的写入命令
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor")
                    // ES7统一type就是_doc，不再允许指定type
                    //.type("readingdata")
                    .source(dataSource);

            // 用index发送请求
            requestIndexer.add(indexRequest);
        }
    }
}

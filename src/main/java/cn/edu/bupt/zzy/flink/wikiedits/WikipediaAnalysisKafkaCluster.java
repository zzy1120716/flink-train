package cn.edu.bupt.zzy.flink.wikiedits;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * @description: 集群模式，并将统计结果写入Kafka
 * @author: zzy
 * @date: 2019-05-20 18:04
 **/
public class WikipediaAnalysisKafkaCluster {

    public static void main(String[] args) throws Exception {

        // 第一步：创建一个StreamExecutionEnvironment
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        // 第二步：创建一个从Wikipedia IRC日志中读取的source
        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        // 第三步：获取用户名作为key，提供一个KeySelector
        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
                .keyBy(new KeySelector<WikipediaEditEvent, String>() {
                    @Override
                    public String getKey(WikipediaEditEvent event) {
                        return event.getUser();
                    }
                });

        // 第四步：在流上加一个窗口，窗口指定要在其上执行计算的Stream的切片，每五秒聚合一次编辑的字节总和
        DataStream<Tuple2<String, Long>> result = keyedEdits
                .timeWindow(Time.seconds(5))
                .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {

                        acc.f0 = event.getUser();
                        acc.f1 += event.getByteDiff();
                        return acc;
                    }
                });

        // 第五步：结果输出到Kafka
        result
                .map(new MapFunction<Tuple2<String,Long>, String>() {
                    @Override
                    public String map(Tuple2<String, Long> tuple) {
                        return tuple.toString();
                    }
                })
                .addSink(new FlinkKafkaProducer09<String>("hadoop000:9092", "wiki-result", new SimpleStringSchema()));
        see.execute();

    }
}

package cn.edu.bupt.zzy.flink.wikiedits;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * @description: 统计用户编辑wikipedia字节数
 * @author: zzy
 * @date: 2019-05-20 18:04
 **/
public class WikipediaAnalysis {

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

        // 第五步：在控制台打印结果，并开始执行
        result.print();
        see.execute();

    }
}

package org.quantum.flink.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.quantum.flink.function.TaskCountingAggregator;
import org.quantum.flink.function.TaskStatisticCollector;
import org.quantum.flink.model.TaskChange;
import org.quantum.flink.model.TaskChangeDeserializationSchema;
import org.quantum.flink.model.TaskStatistic;

import java.util.Properties;

@Slf4j
public class OrderInsightJob {

    public static void main(String[] args) throws Exception {
        log.info("OrderInsightJob started");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KafkaSourceBuilder<TaskChange> builder = KafkaSource.builder();
        final KafkaSource<TaskChange> source = builder
            .setProperties(getKafkaProperties())
            .setTopics("work-order")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new TaskChangeDeserializationSchema())
            .build();
        final DataStreamSource<TaskChange> taskChanges = env.fromSource(source, WatermarkStrategy.noWatermarks(),
            "Kafka Source");
        taskChanges.print();
        final SingleOutputStreamOperator<TaskStatistic> aggregate = taskChanges.keyBy(
                taskChange -> String.join("-", taskChange.getOperator(), String.valueOf(taskChange.getStatus())))
//            .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
            .window(SlidingProcessingTimeWindows.of(Time.minutes(30), Time.minutes(1)))
            .aggregate(new TaskCountingAggregator(), new TaskStatisticCollector());
        aggregate.print();

        env.execute("Order Insight Job");

    }

    static Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "order-insight");
        return properties;
    }


}

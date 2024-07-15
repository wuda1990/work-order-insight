package org.quantum.flink.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.quantum.flink.model.TaskChange;
import org.quantum.flink.model.TaskChangeDeserializationSchema;

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
        final DataStream<TaskChange> broadcast = taskChanges.broadcast();
        broadcast.filter(taskChange -> taskChange.getNewOperator() != null && taskChange.getNewStatus() != null)
            .keyBy(taskChange -> String.join("-", taskChange.getNewOperator(), taskChange.getNewStatus().toString()))
            .window(TumblingProcessingTimeWindows.of(Time.hours(1)));
        ;
        //aggregate by current operator and task status

        env.execute("Order Insight Job");

    }

    static Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "order-insight");
        return properties;
    }


}

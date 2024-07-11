package org.quantum.flink.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.quantum.flink.model.TaskChange;
import org.quantum.flink.model.TaskChangeDeserializationSchema;

import java.util.Properties;

@Slf4j
public class OrderInsightJob {

    public static void main(String[] args) {
        log.info("OrderInsightJob started");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KafkaSourceBuilder<TaskChange> builder = KafkaSource.builder();
        builder.setValueOnlyDeserializer(new TaskChangeDeserializationSchema())
            .setProperties(getKafkaProperties())
            .build();

    }

    static Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "order-insight");
        properties.put("topic", "work-order");
        return properties;
    }


}

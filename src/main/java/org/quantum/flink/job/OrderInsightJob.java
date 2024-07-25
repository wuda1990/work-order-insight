package org.quantum.flink.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
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
import org.quantum.flink.model.OperatorStatistic;
import org.quantum.flink.model.TaskChange;
import org.quantum.flink.model.TaskChangeDeserializationSchema;

import java.util.Properties;

@Slf4j
public class OrderInsightJob {

    public static void main(String[] args) throws Exception {
        log.info("OrderInsightJob started");
        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
            configuration);
        env.setParallelism(4);
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
        final SingleOutputStreamOperator<OperatorStatistic> aggregate = taskChanges.keyBy(
                taskChange -> String.join("-", taskChange.getOperator(), String.valueOf(taskChange.getStatus())))
//            .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
            .window(SlidingProcessingTimeWindows.of(Time.minutes(30), Time.minutes(1)))
            .aggregate(new TaskCountingAggregator(), new TaskStatisticCollector());
        aggregate.print();
        aggregate.addSink(JdbcSink.sink(
            "INSERT INTO t_statistics_operator (operator, status, count) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE count = VALUES(count)",
            (JdbcStatementBuilder<OperatorStatistic>) (ps, t) -> {
                ps.setString(1, t.getOperator());
                ps.setInt(2, t.getStatus());
                ps.setLong(3, t.getCount());
            }, JdbcExecutionOptions.builder()
                .withBatchIntervalMs(200)             // optional: default = 0, meaning no time-based execution is done
                .withBatchSize(1)                  // optional: default = 5000 values
                .withMaxRetries(3)                    // optional: default = 3
                .build(), getJdbcConnectionOptions()));

        env.execute("Order Insight Job");

    }

    static Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "order-insight");
        return properties;
    }

    static JdbcConnectionOptions getJdbcConnectionOptions() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl("jdbc:mysql://localhost:3306/work_order_insight")
            .withDriverName("com.mysql.cj.jdbc.Driver")
            .withUsername("root")
            .build();
    }


}

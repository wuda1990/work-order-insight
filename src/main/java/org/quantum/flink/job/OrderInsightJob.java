package org.quantum.flink.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
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
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.quantum.flink.function.DepartmentStatisticCollector;
import org.quantum.flink.function.OperatorStatisticCollector;
import org.quantum.flink.function.TaskCountingAggregator;
import org.quantum.flink.model.DepartmentStatistic;
import org.quantum.flink.model.OperatorStatistic;
import org.quantum.flink.model.TaskChange;
import org.quantum.flink.model.TaskChangeDeserializationSchema;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.Properties;

@Slf4j
public class OrderInsightJob {

    public static void main(String[] args) throws Exception {
        log.info("OrderInsightJob started");
        String configDirectory = "src/main/resources";
        Configuration configuration = GlobalConfiguration.loadConfiguration(configDirectory);
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
        WatermarkStrategy<TaskChange> watermarkStrategy = WatermarkStrategy
            .<TaskChange>forBoundedOutOfOrderness(Duration.ofMillis(200))
            .withTimestampAssigner((taskChange, l) ->
                taskChange.getChangeDt().atZone(ZoneOffset.systemDefault()).toInstant().toEpochMilli())
            .withIdleness(Duration.ofSeconds(5));
        final DataStreamSource<TaskChange> taskChanges = env.fromSource(source, watermarkStrategy,
            "Kafka Source");
        taskChanges.print();
        final SingleOutputStreamOperator<OperatorStatistic> operatorAggregator = taskChanges.keyBy(
                taskChange -> String.join("-", taskChange.getOperator(), String.valueOf(taskChange.getStatus())))
//            .window(SlidingProcessingTimeWindows.of(Time.minutes(30), Time.minutes(1)))
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .aggregate(new TaskCountingAggregator(), new OperatorStatisticCollector())
            .name("Operator-Aggregator");
        operatorAggregator.print();
        operatorAggregator.addSink(getOperatorStatisticSink()).name("Operator-Sink");

        final SingleOutputStreamOperator<DepartmentStatistic> departmentAggregator = taskChanges.keyBy(
                taskChange -> String.join("-", String.valueOf(taskChange.getDepartment()),
                    String.valueOf(taskChange.getStatus())))
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .aggregate(new TaskCountingAggregator(), new DepartmentStatisticCollector())
            .name("Department-Aggregator");
        departmentAggregator.print();
        departmentAggregator.addSink(getDepartmentStatisticSink()).name("Department-Sink");
        
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

    static SinkFunction<OperatorStatistic> getOperatorStatisticSink() {
        return JdbcSink.sink(
            "INSERT INTO t_statistics_operator (operator, status, count) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE count = VALUES(count)",
            (JdbcStatementBuilder<OperatorStatistic>) (ps, t) -> {
                ps.setString(1, t.getOperator());
                ps.setInt(2, t.getStatus());
                ps.setLong(3, t.getCount());
            }, JdbcExecutionOptions.builder()
                .withBatchIntervalMs(200)             // optional: default = 0, meaning no time-based execution is done
                .withBatchSize(1)                  // optional: default = 5000 values
                .withMaxRetries(3)                    // optional: default = 3
                .build(), getJdbcConnectionOptions());
    }

    static SinkFunction<DepartmentStatistic> getDepartmentStatisticSink() {
        return JdbcSink.sink(
            "INSERT INTO t_statistics_department (department, status, count) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE count = VALUES(count)",
            (JdbcStatementBuilder<DepartmentStatistic>) (ps, t) -> {
                ps.setInt(1, t.getDepartment());
                ps.setInt(2, t.getStatus());
                ps.setLong(3, t.getCount());
            }, JdbcExecutionOptions.builder()
                .withBatchIntervalMs(200)             // optional: default = 0, meaning no time-based execution is done
                .withBatchSize(1)                  // optional: default = 5000 values
                .withMaxRetries(3)                    // optional: default = 3
                .build(), getJdbcConnectionOptions());
    }

}

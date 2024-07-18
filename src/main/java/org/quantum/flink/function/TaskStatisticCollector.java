package org.quantum.flink.function;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.quantum.flink.model.TaskStatistic;

import java.util.Date;

public class TaskStatisticCollector extends ProcessWindowFunction<Long, TaskStatistic, String, TimeWindow> {

    @Override
    public void process(final String operator,
        final ProcessWindowFunction<Long, TaskStatistic, String, TimeWindow>.Context context,
        final Iterable<Long> elements, final Collector<TaskStatistic> out) throws Exception {
        final Long count = elements.iterator().next();
        out.collect(
            new TaskStatistic(new Date(context.window().getStart()), new Date(context.window().getEnd()), operator,
                count));
    }
}

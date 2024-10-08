package org.quantum.flink.function;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.quantum.flink.model.OperatorStatistic;

import java.util.Date;

@Slf4j
public class OperatorStatisticCollector extends ProcessWindowFunction<Long, OperatorStatistic, String, TimeWindow> {

    @Override
    public void process(final String key,
        final ProcessWindowFunction<Long, OperatorStatistic, String, TimeWindow>.Context context,
        final Iterable<Long> elements, final Collector<OperatorStatistic> out) {
        final Long count = elements.iterator().next();
        final String[] split = key.split("-");
        if (split.length != 2) {
            return;
        }
        final OperatorStatistic operatorStatistic = new OperatorStatistic(new Date(context.window().getStart()),
            new Date(context.window().getEnd()), split[0],
            Integer.valueOf(split[1]), count);
        log.debug("OperatorStatistic: {}", operatorStatistic);
        out.collect(operatorStatistic);
    }
}

package org.quantum.flink.function;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.quantum.flink.model.DepartmentStatistic;

import java.util.Date;

@Slf4j
public class DepartmentStatisticCollector extends ProcessWindowFunction<Long, DepartmentStatistic, String, TimeWindow> {

    @Override
    public void process(final String key,
        final ProcessWindowFunction<Long, DepartmentStatistic, String, TimeWindow>.Context context,
        final Iterable<Long> elements, final Collector<DepartmentStatistic> out) {
        final Long count = elements.iterator().next();
        final String[] split = key.split("-");
        if (split.length != 2) {
            return;
        }
        final DepartmentStatistic departmentStatistic = new DepartmentStatistic(new Date(context.window().getStart()),
            new Date(context.window().getEnd()), Integer.valueOf(split[0]),
            Integer.valueOf(split[1]), count);
        log.debug("DepartmentStatistic: {}", departmentStatistic);
        out.collect(departmentStatistic);
    }
}

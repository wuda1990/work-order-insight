package org.quantum.flink.function;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.quantum.flink.model.TaskChange;

@Slf4j
public class TaskCountingAggregator implements AggregateFunction<TaskChange, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(final TaskChange taskChange, final Long accumulator) {
        log.info("TaskChange: {},accumulator:{}", taskChange, accumulator);
        return taskChange.getActive() ? accumulator + 1 : accumulator - 1;
    }

    @Override
    public Long getResult(final Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(final Long a, final Long b) {
        return a + b;
    }
}

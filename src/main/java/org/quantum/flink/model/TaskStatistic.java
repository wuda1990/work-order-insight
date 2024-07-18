package org.quantum.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TaskStatistic {

    private Date windowStart;

    private Date windowEnd;

    private String operator;

    private Long count;
}

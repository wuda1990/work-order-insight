package org.quantum.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DepartmentStatistic {

    private Date windowStart;

    private Date windowEnd;

    private Integer department;

    private Integer status;

    private Long count;
}

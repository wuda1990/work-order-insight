package org.quantum.flink.model;

import lombok.Data;

@Data
public class TaskChange {

    private String taskNo;

    private Byte oldStatus;

    private Byte newStatus;

    private String oldOperator;

    private String newOperator;

    private Integer oldDepartment;

    private Integer newDepartment;


}

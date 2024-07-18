package org.quantum.flink.model;

import lombok.Data;

@Data
public class TaskChange {

    private String taskNo;

    private Byte status;

    private String operator;

    private Integer department;

    private Boolean active;
    
}

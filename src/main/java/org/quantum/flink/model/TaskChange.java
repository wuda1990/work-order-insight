package org.quantum.flink.model;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class TaskChange {

    private String taskNo;

    private Byte status;

    private String operator;

    private Integer department;

    private Boolean active;

    private LocalDateTime changeDt;

}

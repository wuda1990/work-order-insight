package org.quantum.flink.model;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class TaskChangeDeserializationSchema implements DeserializationSchema<TaskChange> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public TaskChange deserialize(final byte[] message) throws IOException {
        return objectMapper.readValue(message, TaskChange.class);
    }

    @Override
    public boolean isEndOfStream(final TaskChange nextElement) {
        return false;
    }

    @Override
    public TypeInformation<TaskChange> getProducedType() {
        return TypeInformation.of(TaskChange.class);
    }
}

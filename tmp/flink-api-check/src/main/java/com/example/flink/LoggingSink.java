package com.example.flink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;

public class LoggingSink implements Sink<String> {

    @Override
    public SinkWriter<String> createWriter(
            WriterInitContext context) throws IOException {
        return new LoggingSinkWriter();
    }
}

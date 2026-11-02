package com.example.flink;

import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;

public class LoggingSinkWriter implements SinkWriter<String> {

    @Override
    public void write(String value, Context context)
            throws IOException {
        System.out.println("[LoggingSink] " + value);
    }

    @Override
    public void flush(boolean endOfInput)
            throws IOException {
        System.out.println(
            "[LoggingSink] flush; endOfInput=" + endOfInput
        );
    }

    @Override
    public void close() {
        System.out.println("[LoggingSink] writer closed");
    }
}

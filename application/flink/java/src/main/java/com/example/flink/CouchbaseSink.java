package com.example.flink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;

public class CouchbaseSink implements Sink<String> {

    private final String connectionString;
    private final String username;
    private final String password;
    private final String bucketName;
    private final String scopeName;
    private final String collectionName;
    private final String documentIdField;

    public CouchbaseSink(
            String connectionString,
            String username,
            String password,
            String bucketName,
            String scopeName,
            String collectionName,
            String documentIdField) {

        this.connectionString = connectionString;
        this.username = username;
        this.password = password;
        this.bucketName = bucketName;
        this.scopeName = scopeName;
        this.collectionName = collectionName;
        this.documentIdField = documentIdField;
    }

    @Override
    public SinkWriter<String> createWriter(
            WriterInitContext context) throws IOException {

        return new CouchbaseSinkWriter(
                connectionString,
                username,
                password,
                bucketName,
                scopeName,
                collectionName,
                documentIdField
        );
    }
}

package com.example.flink;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.time.Duration;

public class CouchbaseSinkWriter implements SinkWriter<String> {

    private final Cluster cluster;
    private final Collection collection;
    private final String documentIdField;

    public CouchbaseSinkWriter(
            String connectionString,
            String username,
            String password,
            String bucketName,
            String scopeName,
            String collectionName,
            String documentIdField) {

        this.documentIdField = documentIdField;

        this.cluster = Cluster.connect(
                connectionString,
                username,
                password
        );

        try {
            this.cluster.waitUntilReady(Duration.ofSeconds(15));

            Bucket bucket = cluster.bucket(bucketName);

            this.collection = bucket
                    .scope(scopeName)
                    .collection(collectionName);
        } catch (RuntimeException e) {
            cluster.disconnect();
            throw e;
        }
    }

    @Override
    public void write(String value, Context context) throws IOException {
        try {
            JsonObject document = JsonObject.fromJson(value);
            Object documentId = document.get(documentIdField);

            if (documentId == null || documentId.toString().isBlank()) {
                throw new IOException(
                        "Missing document ID field: " + documentIdField
                );
            }

            collection.upsert(documentId.toString(), document);
        } catch (IOException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new IOException("Failed to write document to Couchbase", e);
        }
    }

    @Override
    public void flush(boolean endOfInput) {
        // Couchbase upsert is synchronous; each write completes before returning.
    }

    @Override
    public void close() {
        cluster.disconnect();
    }
}

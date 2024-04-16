package com.aerospike.comparator;

import java.io.IOException;

import com.aerospike.client.Key;
import com.aerospike.comparator.dbaccess.RecordMetadata;

public interface RecordDifferenceHandler {
    void handle(int partitionId, Key key, DifferenceCollection differences, RecordMetadata[] recordMetadatas) throws IOException;
}

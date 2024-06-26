package com.aerospike.comparator;

import java.io.IOException;
import java.util.List;

import com.aerospike.client.Key;
import com.aerospike.comparator.dbaccess.RecordMetadata;

public interface MissingRecordHandler {
    void handle(int partitionId, Key key, List<Integer> missingFromClusters, boolean hasRecordLevelDifferences, RecordMetadata[] recordMetadatas) throws IOException;
    default void close() {}
}

package com.aerospike.comparator;

import java.io.IOException;
import java.util.List;

import com.aerospike.client.Key;
import com.aerospike.comparator.dbaccess.RecordMetadata;

public interface RecordDifferenceHandler {
    void handle(int partitionId, Key key, DifferenceCollection differences, List<Integer> missingFromClusters, RecordMetadata[] recordMetadatas) throws IOException;
}

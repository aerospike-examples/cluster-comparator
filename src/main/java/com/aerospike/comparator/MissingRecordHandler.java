package com.aerospike.comparator;

import java.io.IOException;
import java.util.List;

import com.aerospike.client.Key;

public interface MissingRecordHandler {
    void handle(int partitionId, Key key, List<Integer> missingFromClusters) throws IOException;
    default void close() {}
}

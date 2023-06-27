package com.aerospike.comparator;

import java.io.IOException;

import com.aerospike.client.Key;

public interface MissingRecordHandler {
    void handle(int partitionId, Key key, Side missingFromSide) throws IOException;
    default void close() {}
}

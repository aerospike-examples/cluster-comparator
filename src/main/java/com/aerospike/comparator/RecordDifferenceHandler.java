package com.aerospike.comparator;

import java.io.IOException;

import com.aerospike.client.Key;
import com.aerospike.client.Record;

public interface RecordDifferenceHandler {
    void handle(int partitionId, Key key, Record side1, Record side2, DifferenceSet differences) throws IOException;
}

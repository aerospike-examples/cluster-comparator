package com.aerospike.comparator;

import java.util.concurrent.atomic.AtomicLongArray;

public class DifferenceSummary {
    private final long differences;
    private final AtomicLongArray missingRecords;
    
    public DifferenceSummary(AtomicLongArray missingRecords, long differences) {
        super();
        this.missingRecords = missingRecords;
        this.differences = differences;
    }

    public long getMissingRecords(int clusterId) {
        return missingRecords.get(clusterId);
    }

    public long getDifferences() {
        return differences;
    }
    
    public boolean areDifferent() {
        for (int i = 0; i < missingRecords.length(); i++) {
            if (missingRecords.get(i) > 0) {
                return true;
            }
        }
        return differences > 0;
    }
}

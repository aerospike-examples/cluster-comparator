package com.aerospike.comparator;

public class DifferenceSummary {
    private final long missingRecordsSide1;
    private final long missingRecordsSide2;
    private final long differences;
    
    public DifferenceSummary(long missingRecordsSide1, long missingRecordsSide2, long differences) {
        super();
        this.missingRecordsSide1 = missingRecordsSide1;
        this.missingRecordsSide2 = missingRecordsSide2;
        this.differences = differences;
    }

    public long getMissingRecordsSide1() {
        return missingRecordsSide1;
    }

    public long getMissingRecordsSide2() {
        return missingRecordsSide2;
    }

    public long getDifferences() {
        return differences;
    }
    
    public boolean areDifferent() {
        return missingRecordsSide1 > 0 || missingRecordsSide2 > 0 || differences > 0;
    }
}

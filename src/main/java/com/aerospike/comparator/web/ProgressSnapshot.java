package com.aerospike.comparator.web;

public class ProgressSnapshot {
    private final long[] recordsProcessedPerCluster;
    private final long[] recordsMissingPerCluster;
    private final long recordsDifferent;
    private final long totalMissingRecords;
    private final long totalRecordsCompared;
    private final int partitionsComplete;
    private final int totalPartitions;
    private final boolean forceTerminated;
    private final String outputFile;
    private String state;
    private long completedAt;

    public ProgressSnapshot(long[] recordsProcessedPerCluster, long[] recordsMissingPerCluster,
            long recordsDifferent, long totalMissingRecords, long totalRecordsCompared,
            int partitionsComplete, int totalPartitions, boolean forceTerminated, String outputFile) {
        this.recordsProcessedPerCluster = recordsProcessedPerCluster;
        this.recordsMissingPerCluster = recordsMissingPerCluster;
        this.recordsDifferent = recordsDifferent;
        this.totalMissingRecords = totalMissingRecords;
        this.totalRecordsCompared = totalRecordsCompared;
        this.partitionsComplete = partitionsComplete;
        this.totalPartitions = totalPartitions;
        this.forceTerminated = forceTerminated;
        this.outputFile = outputFile;
    }

    public long[] getRecordsProcessedPerCluster() {
        return recordsProcessedPerCluster;
    }

    public long[] getRecordsMissingPerCluster() {
        return recordsMissingPerCluster;
    }

    public long getRecordsDifferent() {
        return recordsDifferent;
    }

    public long getTotalMissingRecords() {
        return totalMissingRecords;
    }

    public long getTotalRecordsCompared() {
        return totalRecordsCompared;
    }

    public int getPartitionsComplete() {
        return partitionsComplete;
    }

    public int getTotalPartitions() {
        return totalPartitions;
    }

    public boolean isForceTerminated() {
        return forceTerminated;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public long getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(long completedAt) {
        this.completedAt = completedAt;
    }
}

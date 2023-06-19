package com.aerospike.comparator.metadata;

public class NamespaceConfig {
    public enum ConflictResolutionPolicy {
        GENERATION("generation"),
        LAST_UPDATE_TIME("last-update-time");
        
        private String value;
        private ConflictResolutionPolicy(String value) {
            this.value = value;
        }
        public boolean matches(String value) {
            return this.value.equals(value);
        }
    }
    
    public enum WriteCommitLevelOverride {
        OFF,
        ALL,
        MASTER
    }
    
    public enum ReadConsistencyLevelOverride {
        OFF,
        ONE,
        ALL
    }
    @FieldName(value = "conflict-resolve-writes", defValue = "false") private boolean conflictResolveWrites; 
    @FieldName(value = "data-in-index", defValue = "false") private boolean dataInIndex; 
    @FieldName(value = "default-ttl", defValue = "0") private long defaultTtl; 
    @FieldName(value = "disable-cold-start-eviction", defValue = "false") private boolean disableColdStartEviction; 
    @FieldName(value = "disable-write-dup-res", defValue = "false") private boolean disableWriteDupRes; 
    @FieldName(value = "disallow-expunge", defValue = "false") private boolean disallowExpunge; 
    @FieldName(value = "disallow-null-setname", defValue = "false") private boolean disallowNullSetname; 
    @FieldName(value = "enable-benchmarks-batch-sub", defValue = "false") private boolean enableBenchmarksBatchSub; 
    @FieldName(value = "enable-benchmarks-ops-sub", defValue = "false") private boolean enableBenchmarksOpsSub; 
    @FieldName(value = "enable-benchmarks-read", defValue = "false") private boolean enableBenchmarksRead; 
    @FieldName(value = "enable-benchmarks-udf", defValue = "false") private boolean enableBenchmarksUdf; 
    @FieldName(value = "enable-benchmarks-udf-sub", defValue = "false") private boolean enableBenchmarksUdfSub; 
    @FieldName(value = "enable-benchmarks-write", defValue = "false") private boolean enableBenchmarksWrite; 
    @FieldName(value = "enable-hist-proxy", defValue = "false") private boolean enableHistProxy; 
    @FieldName(value = "evict-hist-buckets", defValue = "10000") private long evictHistBuckets; 
    @FieldName(value = "evict-tenths-pct", defValue = "5") private int evictTenthsPct; 
    @FieldName(value = "force-long-queries", defValue = "false") private boolean forceLongQueries; 
    @FieldName(value = "high-water-disk-pct", defValue = "0") private int highWaterDiskPct; 
    @FieldName(value = "high-water-memory-pct", defValue = "0") private int highWaterMemoryPct; 
    @FieldName(value = "ignore-migrate-fill-delay", defValue = "false") private boolean ignoreMigrateFillDelay; 
    @FieldName(value = "index-stage-size", defValue = "1073741824") private long indexStageSize; 
    @FieldName(value = "inline-short-queries", defValue = "false") private boolean inlineShortQueries; 
    @FieldName(value = "max-record-size", defValue = "0") private long maxRecordSize; 
    @FieldName(value = "memory-size", defValue = "4294967296") private long memorySize; 
    @FieldName(value = "migrate-order", defValue = "5") private int migrateOrder; 
    @FieldName(value = "migrate-retransmit-ms", defValue = "5000") private long migrateRetransmitMs; 
    @FieldName(value = "migrate-sleep", defValue = "1") private long migrateSleep; 
    @FieldName(value = "nsup-hist-period", defValue = "3600") private long nsupHistPeriod; 
    @FieldName(value = "nsup-period", defValue = "0") private long nsupPeriod; 
    @FieldName(value = "nsup-threads", defValue = "1") private int nsupThreads; 
    @FieldName(value = "partition-tree-sprigs", defValue = "256") private long partitionTreeSprigs; 
    @FieldName(value = "prefer-uniform-balance", defValue = "true") private boolean preferUniformBalance; 
    @FieldName(value = "rack-id", defValue = "0") private boolean rackId; 
    @FieldName(value = "read-consistency-level-override", defValue = "off") private ReadConsistencyLevelOverride readConsistencyLevelOverride; 
    @FieldName(value = "reject-non-xdr-writes", defValue = "false") private boolean rejectNonXdrWrites; 
    @FieldName(value = "reject-xdr-writes", defValue = "false") private boolean rejectXdrWrites; 
    @FieldName(value = "replication-factor", defValue = "2") private int replicationFactor; 
    @FieldName(value = "sindex-stage-size", defValue = "1073741824") private long sindexStageSize; 
    @FieldName(value = "single-bin", defValue = "false") private boolean singleBin; 
    @FieldName(value = "single-query-threads", defValue = "4") private int singleQueryThreads; 
    @FieldName(value = "stop-writes-pct", defValue = "90") private int stopWritesPct; 
    @FieldName(value = "stop-writes-sys-memory-pct", defValue = "90") private int stopWritesSysMemoryPct; 
    @FieldName(value = "strong-consistency", defValue = "false") private boolean strongConsistency; 
    @FieldName(value = "strong-consistency-allow-expunge", defValue = "false") private boolean strongConsistencyAllowExpunge; 
    @FieldName(value = "tomb-raider-eligible-age", defValue = "86400") private long tombRaiderEligibleAge; 
    @FieldName(value = "tomb-raider-period", defValue = "86400") private long tombRaiderPeriod; 
    @FieldName(value = "transaction-pending-limit", defValue = "20") private int transactionPendingLimit; 
    @FieldName(value = "truncate-threads", defValue = "4") private int truncateThreads; 
    @FieldName(value = "write-commit-level-override", defValue = "off") private WriteCommitLevelOverride writeCommitLevelOverride; 
    @FieldName(value = "xdr-bin-tombstone-ttl", defValue = "86400") private int xdrBinTombstoneTtl; 
    @FieldName(value = "xdr-tomb-raider-period", defValue = "120") private long xdrTombRaiderPeriod; 
    @FieldName(value = "xdr-tomb-raider-threads", defValue = "1") private int xdrTombRaiderThreads; 
    @FieldName(value = "geo2dsphere-within.strict", defValue = "true") private boolean geo2dsphereStrict; 
    @FieldName(value = "geo2dsphere-within.min-level", defValue = "1") private int geo2dsphereMinLevel; 
    @FieldName(value = "geo2dsphere-within.max-level", defValue = "20") private int geo2dsphereMaxLevel; 
    @FieldName(value = "geo2dsphere-within.max-cells", defValue = "12") private int geo2dsphereMaxCells; 
    @FieldName(value = "geo2dsphere-within.level-mod", defValue = "1") private int geo2dsphereLevelMod; 
    @FieldName(value = "geo2dsphere-within.earth-radius-meters", defValue = "6371000") private long geo2dsphereEarthRadiusMeters; 
    @FieldName(value = "index-type", defValue = "shmem") private boolean indexType; 
    @FieldName(value = "sindex-type", defValue = "shmem") private boolean sindexType; 
    @FieldName(value = "storage-engine", defValue = "memory}") private boolean storageEngine; 
}

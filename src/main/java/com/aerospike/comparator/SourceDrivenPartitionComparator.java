package com.aerospike.comparator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.Statement;
import com.aerospike.comparator.ClusterComparatorOptions.CompareMode;
import com.aerospike.comparator.dbaccess.AerospikeClientAccess;
import com.aerospike.comparator.dbaccess.RecordMetadata;
import com.aerospike.comparator.dbaccess.RecordSetAccess;

/**
 * Handles source-driven partition comparison for set mapping: scans the designated
 * source cluster, then performs batched lookups on the remaining clusters using
 * mapped set names. Records existing only on non-source clusters are not reported.
 * <p>
 * This is used when set mapping is active ({@code --sourceCluster} with {@code setMapping} config).
 */
class SourceDrivenPartitionComparator {

    private static class SourceRecord {
        final Key key;
        final Record sourceRecord;

        SourceRecord(Key key, Record sourceRecord) {
            this.key = key;
            this.sourceRecord = sourceRecord;
        }
    }

    private final ClusterComparator parent;
    private final ClusterComparatorOptions options;

    SourceDrivenPartitionComparator(ClusterComparator parent) {
        this.parent = parent;
        this.options = parent.getOptions();
    }

    void comparePartition(AerospikeClientAccess[] clients, String namespace, String setName, int partitionId) {
        int sourceClusterIdx = options.getSourceCluster();
        int batchSize = options.getLookupBatchSize();

        QueryPolicy queryPolicy = new QueryPolicy(parent.getQueryPolicyToUse());
        queryPolicy.maxConcurrentNodes = 1;
        queryPolicy.includeBinData = options.isRecordLevelCompare();
        queryPolicy.shortQuery = false;
        queryPolicy.filterExp = parent.getFilterExpression();

        int rps = options.getRps() / parent.getThreadsToUse();
        if (options.getRps() > 0 && rps == 0) {
            rps = 1;
        }

        List<Integer> nonSourceIndices = new ArrayList<>();
        for (int i = 0; i < parent.getNumberOfClusters(); i++) {
            if (i != sourceClusterIdx) {
                nonSourceIndices.add(i);
            }
        }

        String namespaceName = options.getNamespaceName(namespace, sourceClusterIdx);
        Statement stmt = new Statement();
        stmt.setNamespace(namespaceName);
        stmt.setSetName(setName);
        stmt.setRecordsPerSecond(rps);
        PartitionFilter filter = PartitionFilter.id(partitionId);

        if (options.isDebug()) {
            System.out.printf("Thread %d starting set-mapping comparison of namespace %s, set %s, partition %d (source cluster: %d)\n",
                    Thread.currentThread().getId(), namespace, setName, partitionId, sourceClusterIdx);
        }

        RecordSetAccess recordSet = clients[sourceClusterIdx].queryPartitions(queryPolicy, stmt, filter);
        List<SourceRecord> batch = new ArrayList<>(batchSize);

        try {
            while (parent.getNextRecord(recordSet, sourceClusterIdx) && !parent.forceTerminate) {
                Key key = recordSet.getKey();
                Record record = options.isRecordLevelCompare() ? recordSet.getRecord() : null;
                batch.add(new SourceRecord(key, record));

                if (batch.size() >= batchSize) {
                    flushBatch(clients, batch, nonSourceIndices, namespace, setName, partitionId);
                    batch.clear();
                }
            }

            if (!batch.isEmpty() && !parent.forceTerminate) {
                flushBatch(clients, batch, nonSourceIndices, namespace, setName, partitionId);
                batch.clear();
            }
            parent.partitionsComplete[partitionId - parent.getStartPartition()].set(true);
        }
        finally {
            try {
                recordSet.close();
            } catch (Exception ignored) {}
        }
    }

    private void flushBatch(AerospikeClientAccess[] clients, List<SourceRecord> batch,
            List<Integer> nonSourceIndices, String namespace, String setName, int partitionId) {

        boolean recordLevelCompare = options.isRecordLevelCompare();
        BatchPolicy batchPolicy = new BatchPolicy(parent.getReadPolicyToUse());
        int sourceClusterIdx = options.getSourceCluster();

        Record[][] nonSourceRecords = new Record[parent.getNumberOfClusters()][];
        boolean[][] nonSourceExists = new boolean[parent.getNumberOfClusters()][];

        for (int clusterIdx : nonSourceIndices) {
            String resolvedNamespace = options.getNamespaceName(namespace, clusterIdx);
            String resolvedSet = options.getSetName(setName, clusterIdx);
            boolean setMapped = !resolvedSet.equals(setName);

            Key[] lookupKeys = new Key[batch.size()];
            boolean[] validKeys = new boolean[batch.size()];

            for (int b = 0; b < batch.size(); b++) {
                SourceRecord sr = batch.get(b);
                if (setMapped) {
                    if (sr.key.userKey == null) {
                        validKeys[b] = false;
                        if (options.isVerbose()) {
                            System.out.printf("Warning: Skipping record with digest %s - set mapping requires stored user key\n",
                                    Arrays.toString(sr.key.digest));
                        }
                        continue;
                    }
                    lookupKeys[b] = new Key(resolvedNamespace, resolvedSet, sr.key.userKey);
                }
                else {
                    lookupKeys[b] = new Key(resolvedNamespace, sr.key.digest, sr.key.setName, sr.key.userKey);
                }
                validKeys[b] = true;
            }

            Key[] validLookupKeys = filterValidKeys(lookupKeys, validKeys);
            if (validLookupKeys.length == 0) {
                continue;
            }

            if (recordLevelCompare) {
                Record[] results = clients[clusterIdx].get(batchPolicy, validLookupKeys);
                nonSourceRecords[clusterIdx] = expandResults(results, validKeys, batch.size());
            }
            else {
                boolean[] results = clients[clusterIdx].exists(batchPolicy, validLookupKeys);
                nonSourceExists[clusterIdx] = expandBoolResults(results, validKeys, batch.size());
            }
        }

        RecordComparator comparator = new RecordComparator();

        for (int b = 0; b < batch.size(); b++) {
            if (parent.forceTerminate) break;
            SourceRecord sr = batch.get(b);

            List<Integer> clustersWithRecord = new ArrayList<>();
            clustersWithRecord.add(sourceClusterIdx);
            List<Integer> clustersWithoutRecord = new ArrayList<>();

            for (int clusterIdx : nonSourceIndices) {
                boolean exists;
                if (recordLevelCompare) {
                    exists = nonSourceRecords[clusterIdx] != null && nonSourceRecords[clusterIdx][b] != null;
                }
                else {
                    exists = nonSourceExists[clusterIdx] != null && nonSourceExists[clusterIdx][b];
                }
                if (exists) {
                    clustersWithRecord.add(clusterIdx);
                }
                else {
                    clustersWithoutRecord.add(clusterIdx);
                }
            }

            if (options.getCompareMode() == CompareMode.FIND_OVERLAP) {
                if (clustersWithRecord.size() > 1) {
                    parent.missingRecord(clients, clustersWithRecord, clustersWithoutRecord, false, partitionId, sr.key);
                }
            }
            else {
                boolean hasRecordLevelDifferences = false;
                if (recordLevelCompare && clustersWithRecord.size() > 1) {
                    Record[] allRecords = new Record[parent.getNumberOfClusters()];
                    allRecords[sourceClusterIdx] = sr.sourceRecord;
                    for (int clusterIdx : nonSourceIndices) {
                        if (nonSourceRecords[clusterIdx] != null) {
                            allRecords[clusterIdx] = nonSourceRecords[clusterIdx][b];
                        }
                    }
                    hasRecordLevelDifferences = compareRecords(comparator, partitionId, clustersWithRecord, clients, sr.key, allRecords);
                }
                if (!clustersWithoutRecord.isEmpty()) {
                    parent.missingRecord(clients, clustersWithRecord, clustersWithoutRecord, hasRecordLevelDifferences, partitionId, sr.key);
                }
            }
        }
    }

    private boolean compareRecords(RecordComparator comparator, int partitionId,
            List<Integer> clustersWithRecord, AerospikeClientAccess[] clients, Key key, Record[] allRecords) {
        DifferenceCollection compareResult = new DifferenceCollection(clustersWithRecord);
        for (int i = 0; i < clustersWithRecord.size(); i++) {
            for (int j = i + 1; j < clustersWithRecord.size(); j++) {
                int left = clustersWithRecord.get(i);
                int right = clustersWithRecord.get(j);
                Record rec1 = allRecords[left];
                Record rec2 = allRecords[right];
                if (rec1 != null && rec2 != null) {
                    DifferenceSet diffSet = comparator.compare(key, rec1, rec2,
                            options.getPathOptions(),
                            options.getCompareMode() == CompareMode.RECORDS_DIFFERENT, left, right);
                    compareResult.add(diffSet);
                }
            }
        }

        if (compareResult.hasDifferences()) {
            RecordMetadata[] recordMetadatas = parent.getMetadata(clients, key, null, clustersWithRecord);
            parent.differentRecords(partitionId, key, compareResult, parent.invertClusterList(clustersWithRecord), recordMetadatas);
        }
        long totalRecsCompared = parent.totalRecordsCompared.incrementAndGet();
        if (options.getRecordCompareLimit() > 0 && totalRecsCompared >= options.getRecordCompareLimit()) {
            parent.forceTerminate = true;
        }
        return compareResult.hasDifferences();
    }

    private static Key[] filterValidKeys(Key[] keys, boolean[] valid) {
        int count = 0;
        for (boolean v : valid) {
            if (v) count++;
        }
        Key[] result = new Key[count];
        int idx = 0;
        for (int i = 0; i < keys.length; i++) {
            if (valid[i]) {
                result[idx++] = keys[i];
            }
        }
        return result;
    }

    private static Record[] expandResults(Record[] compressed, boolean[] valid, int fullSize) {
        Record[] result = new Record[fullSize];
        int idx = 0;
        for (int i = 0; i < fullSize; i++) {
            if (valid[i]) {
                result[i] = idx < compressed.length ? compressed[idx++] : null;
            }
        }
        return result;
    }

    private static boolean[] expandBoolResults(boolean[] compressed, boolean[] valid, int fullSize) {
        boolean[] result = new boolean[fullSize];
        int idx = 0;
        for (int i = 0; i < fullSize; i++) {
            if (valid[i]) {
                result[i] = idx < compressed.length ? compressed[idx++] : false;
            }
        }
        return result;
    }
}

package com.aerospike.comparator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.comparator.dbaccess.AerospikeClientAccess;

public class PartitionMap {
    private static final int NUMBER_OF_PARTITIONS = 4096;
    
    private final Map<String, List<PartitionData>> namespaceToPartitions = new HashMap<>();
    private final Map<String, Boolean> complete = new HashMap<>();
    
    public PartitionMap(AerospikeClientAccess client) {
        Map<String, String> nodeToInfo = client.invokeInfoCommandOnAllNodes("partition-info");
        for (String nodeName : nodeToInfo.keySet()) {
            String result = nodeToInfo.get(nodeName);
            String[] entries = result.split(";");
            if (entries.length == 0) {
                continue;
            }
            String headerLine = entries[0].trim();
            for (int i = 1; i < entries.length; i++) {
                String line = entries[i].trim();
                if (line.isEmpty()) {
                    continue;
                }

                PartitionData data = PartitionData.parse(headerLine, line);
                String namespace = data.getNamespace();
                if (namespaceToPartitions.get(namespace) == null) {
                    List<PartitionData> partData = new ArrayList<>(NUMBER_OF_PARTITIONS);
                    for (int partitionId = 0; partitionId < NUMBER_OF_PARTITIONS; partitionId++) {
                        partData.add(null);
                    }
                    namespaceToPartitions.put(namespace, partData);
                }
                if (nodeName.equals(data.getWorkingMaster())) {
                    namespaceToPartitions.get(namespace).set(data.getPartitionId(), data);
                }
            }
        }
        for (String namespace : namespaceToPartitions.keySet()) {
            boolean isComplete = true;
            List<PartitionData> partitionDataList = namespaceToPartitions.get(namespace);
            for (int i = 0; i < NUMBER_OF_PARTITIONS; i++) {
                if (partitionDataList.get(i) == null) {
                    isComplete = false;
                    break;
                }
            }
            complete.put(namespace, isComplete);
        }
    }
    
    public boolean isComplete(String namespace) {
        return complete.get(namespace) != null && complete.get(namespace);
    }
    
    public boolean isMigrationsHappening(String namespace) {
        List<PartitionData> partitionDataList = namespaceToPartitions.get(namespace);
        for (int i = 0; i < NUMBER_OF_PARTITIONS; i++) {
            if ((partitionDataList.get(i) == null) || (partitionDataList.get(i).getEmigrates() > 0) || (partitionDataList.get(i).getImmigrates() > 0)) {
                return true;
            }
        }
        return false;
    }
    
    public long getRecordCount(String namespace) {
        List<PartitionData> partitionDataList = namespaceToPartitions.get(namespace);
        if (partitionDataList == null || !complete.get(namespace)) {
            return -1;
        }
        else {
            long count = 0;
            for (int i = 0; i < NUMBER_OF_PARTITIONS; i++) {
                count += partitionDataList.get(i).getRecords();
            }
            return count;
        }
    }

    public long getTombstoneCount(String namespace) {
        List<PartitionData> partitionDataList = namespaceToPartitions.get(namespace);
        if (partitionDataList == null || !complete.get(namespace)) {
            return -1;
        }
        else {
            long count = 0;
            for (int i = 0; i < NUMBER_OF_PARTITIONS; i++) {
                count += partitionDataList.get(i).getTombstones();
            }
            return count;
        }
    }
    
    public long getRecords(String namespace, int partitionId) {
        if (!this.isComplete(namespace)) {
            throw new QuickCompareException("Cannot get record count of an incomplete partition map");
        }
        return this.namespaceToPartitions.get(namespace).get(partitionId).getRecords();
    }

    public long getTombstones(String namespace, int partitionId) {
        if (!this.isComplete(namespace)) {
            throw new QuickCompareException("Cannot get tombstone count of an incomplete partition map");
        }
        return this.namespaceToPartitions.get(namespace).get(partitionId).getTombstones();
    }

    public long getNetObjectCount(String namespace, int partitionId) {
        if (!this.isComplete(namespace)) {
            throw new QuickCompareException("Cannot get net object count of an incomplete partition map");
        }
        return getRecords(namespace, partitionId) - getTombstones(namespace, partitionId);
    }
    
    public List<Integer> compare(PartitionMap partMap, String namespace) {
        return compare(partMap, namespace, namespace);
    }

    /**
     * Compare two partition maps when each side uses its own namespace name.
     * Namespace mapping stores counts under the name that exists on that cluster.
     */
    public List<Integer> compare(PartitionMap partMap, String thisNamespace, String otherNamespace) {
        if (!this.isComplete(thisNamespace) || !partMap.isComplete(otherNamespace)) {
            throw new QuickCompareException("Partition maps can only be compared for maps which are complete");
        }
        List<Integer> results = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_PARTITIONS; i++) {
            if (getNetObjectCount(thisNamespace, i) != partMap.getNetObjectCount(otherNamespace, i)) {
                results.add(i);
            }
        }
        return results;
    }
}

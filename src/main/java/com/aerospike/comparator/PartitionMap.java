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
            String[] lines = result.split(";" );
            boolean first = true;
            for (String line: lines) {
                if (first) {
                    // This is a headng line, skip it
                    first = false;
                    continue;
                }
                
                PartitionData data = new PartitionData(line);
                String namespace = data.getNamespace();
                if (namespaceToPartitions.get(namespace) == null) {
                    List<PartitionData> partData = new ArrayList<>(NUMBER_OF_PARTITIONS);
                    for (int i = 0; i < NUMBER_OF_PARTITIONS; i++) {
                        partData.add(null);
                    }
                    namespaceToPartitions.put(namespace, partData);
                }
                if (nodeName.equals(data.getWokingMaster())) {
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
    
    public long getNetObjectCount(String namespace, int partitionId) {
        if (!this.isComplete(namespace)) {
            throw new QuickCompareException("Cannot get net object count of an incomplete partition map");
        }
        PartitionData data = this.namespaceToPartitions.get(namespace).get(partitionId);
        return data.getRecords() - data.getTombstones();
    }
    
    public List<Integer> compare(PartitionMap partMap, String namespace) {
        if (!this.isComplete(namespace) || !partMap.isComplete(namespace)) {
            throw new QuickCompareException("Partition maps can only be compared for maps which are complete");
        }
        List<Integer> results = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_PARTITIONS; i++) {
            if (getNetObjectCount(namespace, i) != partMap.getNetObjectCount(namespace, i)) {
                results.add(i);
            }
        }
        return results;
    }
}

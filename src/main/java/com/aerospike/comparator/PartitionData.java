package com.aerospike.comparator;

public class PartitionData {
    private final String namespace;
    private final int partitionId;
    private final String state;
    private final int nReplicas;
    private final int replica;
    private final int nDupl;
    private final String workingMaster;
    private final long emigrates;
    private final long leadEmigrates;
    private final long immigrates;
    private final long records;
    private final long tombstones;
    
    public PartitionData(String data) {
        String[] cols = data.split(":");
        namespace = cols[0];
        partitionId = Integer.parseInt(cols[1]);
        state = cols[2];
        nReplicas = Integer.parseInt(cols[3]);
        replica = Integer.parseInt(cols[4]);
        nDupl = Integer.parseInt(cols[5]);
        workingMaster = cols[6];
        emigrates = Long.parseLong(cols[7]);
        leadEmigrates = Long.parseLong(cols[8]);
        immigrates = Long.parseLong(cols[9]);
        records = Long.parseLong(cols[10]);
        tombstones = Long.parseLong(cols[11]);
    }

    public String getNamespace() {
        return namespace;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getState() {
        return state;
    }

    public int getnReplicas() {
        return nReplicas;
    }

    public int getReplica() {
        return replica;
    }

    public int getnDupl() {
        return nDupl;
    }

    public String getWorkingMaster() {
        return workingMaster;
    }

    public long getEmigrates() {
        return emigrates;
    }

    public long getLeadEmigrates() {
        return leadEmigrates;
    }

    public long getImmigrates() {
        return immigrates;
    }

    public long getRecords() {
        return records;
    }

    public long getTombstones() {
        return tombstones;
    }
    
}
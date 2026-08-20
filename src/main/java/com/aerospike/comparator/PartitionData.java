package com.aerospike.comparator;

import java.util.HashMap;
import java.util.Map;

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

    /**
     * Build the field-name to column-index map from the heading line which the
     * server returns as the first entry of the {@code partition-info} response.
     * <p>
     * The server has added fields to this response over time ({@code succession},
     * {@code proxy_dst} and {@code tree_id} are all present in 8.1 but absent in
     * 6.4), so column positions cannot be assumed. Resolving fields by name keeps
     * parsing correct across server versions.
     */
    public static Map<String, Integer> parseHeader(String header) {
        Map<String, Integer> fieldIndex = new HashMap<>();
        String[] names = header.split(":");
        for (int i = 0; i < names.length; i++) {
            fieldIndex.put(names[i].trim(), i);
        }
        return fieldIndex;
    }

    public PartitionData(String data, Map<String, Integer> fieldIndex) {
        String[] cols = data.split(":");
        namespace = getString(cols, fieldIndex, "namespace", "");
        partitionId = (int) getLong(cols, fieldIndex, "partition", -1);
        state = getString(cols, fieldIndex, "state", "");
        nReplicas = (int) getLong(cols, fieldIndex, "n_replicas", 0);
        replica = (int) getLong(cols, fieldIndex, "replica", 0);
        nDupl = (int) getLong(cols, fieldIndex, "n_dupl", 0);
        workingMaster = getString(cols, fieldIndex, "working_master", "");
        emigrates = getLong(cols, fieldIndex, "emigrates", 0);
        leadEmigrates = getLong(cols, fieldIndex, "lead_emigrates", 0);
        immigrates = getLong(cols, fieldIndex, "immigrates", 0);
        records = getLong(cols, fieldIndex, "records", 0);
        tombstones = getLong(cols, fieldIndex, "tombstones", 0);
    }

    private static String getString(String[] cols, Map<String, Integer> fieldIndex, String name, String defaultValue) {
        Integer index = fieldIndex.get(name);
        if (index == null || index >= cols.length) {
            return defaultValue;
        }
        return cols[index];
    }

    private static long getLong(String[] cols, Map<String, Integer> fieldIndex, String name, long defaultValue) {
        String value = getString(cols, fieldIndex, name, null);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        }
        catch (NumberFormatException nfe) {
            return defaultValue;
        }
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

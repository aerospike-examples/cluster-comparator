package com.aerospike.comparator.metadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MismatchingOnCluster {
    private final String name;
    private final Map<String, String> values = new HashMap<>();
    private int cluster;
    
    public MismatchingOnCluster(final String name) {
        this.name = name;
    }
    
    public MismatchingOnCluster addMismatchingValues(final List<Map<String, String>> contextValues, final List<String> nodeNames, int cluster) {
        if (nodeNames.size() != contextValues.size()) {
            throw new IllegalArgumentException(String.format("Expecting node count (%d) to match the number of contexts (%d)", nodeNames.size(), contextValues.size()));
        }
        for (int i = 0; i < nodeNames.size(); i++) {
            this.values.put(nodeNames.get(i), contextValues.get(i).get(name));
        }
        this.cluster = cluster;
        return this;
    }
    
    public String getName() {
        return name;
    }
    
    public Map<String, String> getValues() {
        return values;
    }
    
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer(name).append(" differs on cluster ").append(cluster).append(". ");
        for (String nodeName : values.keySet()) {
            sb.append(nodeName).append('=').append(values.get(nodeName)).append(';');
        }
        // TODO: We could optimize this to just show the differences, or limit the output.
        return sb.toString();
    }
}

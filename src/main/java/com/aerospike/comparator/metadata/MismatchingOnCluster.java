package com.aerospike.comparator.metadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.cluster.Node;
import com.aerospike.comparator.Side;

public class MismatchingOnCluster {
    private final String name;
    private final Map<String, String> values = new HashMap<>();
    private Side side;
    
    public MismatchingOnCluster(final String name) {
        this.name = name;
    }
    
    public MismatchingOnCluster addMismatchingValues(final List<Map<String, String>> contextValues, final Node[] nodes, Side side) {
        if (nodes.length != contextValues.size()) {
            throw new IllegalArgumentException(String.format("Expecting node count (%d) to match the number of contexts (%d)", nodes.length, contextValues.size()));
        }
        for (int i = 0; i < nodes.length; i++) {
            this.values.put(nodes[i].getName(), contextValues.get(i).get(name));
        }
        this.side = side;
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
        StringBuffer sb = new StringBuffer(name).append(" differs on cluster ").append(side.value).append(". ");
        for (String nodeName : values.keySet()) {
            sb.append(nodeName).append('=').append(values.get(nodeName)).append(';');
        }
        // TODO: We could optimize this to just show the differences, or limit the output.
        return sb.toString();
    }
}

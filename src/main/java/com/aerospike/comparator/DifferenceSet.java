package com.aerospike.comparator;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.aerospike.client.Key;
import com.aerospike.comparator.RecordComparator.DifferenceType;

public class DifferenceSet {
    private final Deque<String> pathParts = new ArrayDeque<>();
    // Differences in the record, keyed by the path. 
    private final Map<String, DifferenceValue> differences = new HashMap<>();
    private final Key key;
    private final boolean quickCompare;
    private final PathOptions pathOptions;
    private final int cluster1Index;
    private final int cluster2Index;
    
    public DifferenceSet() {
        this(null, false, null, -1, -1);
    }
    
    public DifferenceSet(Key key, boolean quickCompare, PathOptions pathOptions, int cluster1Index, int cluster2Index) {
        this.key = key;
        this.quickCompare = quickCompare;
        this.pathOptions = pathOptions;
        this.cluster1Index = cluster1Index;
        this.cluster2Index = cluster2Index;
    }
    
    public EnumSet<PathAction> getOptionsForCurrentPath() {
        if (pathOptions != null) {
            EnumSet<PathAction> options = pathOptions.getActionsForPath(pathParts);
            return options;
        }
        return null;
    }
    
    public boolean shouldIgnoreCurrentPath() {
        if (pathOptions != null) {
            EnumSet<PathAction> options = pathOptions.getActionsForPath(pathParts);
            return options.contains(PathAction.IGNORE);
        }
        return false;
    }
    
    protected void pushPath(String pathPart, String ...otherParts) {
        this.pathParts.addFirst(pathPart);
        for (String thisPart : otherParts) {
            this.pathParts.addFirst(thisPart);
        }
    }

    protected String popPath() {
        return this.pathParts.removeFirst();
    }

    protected void popPath(int count) {
        if (count < 1) {
            throw new IllegalArgumentException("count must be >= 1");
        }
        for (int i = 0; i < count; i++) {
            this.pathParts.removeFirst();
        }
    }

    protected String getCurrentPath() {
        StringBuilder sb = new StringBuilder(1000);
        for (Iterator<String> iter = pathParts.descendingIterator(); iter.hasNext();) {
            sb.append(iter.next()).append("/");
        }
        return sb.toString();
    }

    public void addDifference(String path, DifferenceType difference, Object obj1, Object obj2, int cluster1, int cluster2) {
        String fullPath = pathParts.size() == 0 ? path : this.getCurrentPath() + path;
        this.differences.put(fullPath, new DifferenceValue(difference, obj1, obj2, cluster1, cluster2));
    }

    public void addDifference(String path, DifferenceType difference, Object obj1, Object obj2, int index, int cluster1, int cluster2) {
        String fullPath = pathParts.size() == 0 ? path : this.getCurrentPath() + path;
        this.differences.put(fullPath, new DifferenceValue(difference, obj1, obj2, index, cluster1, cluster2));
    }
    public Map<String, DifferenceValue> getDifferences() {
        return differences;
    }

    public boolean areDifferent() {
        return !this.differences.isEmpty();
    }
    
    public boolean isQuickCompare() {
        return quickCompare;
    }
    
    public String getAsJson(boolean truncateBinary, ClusterComparatorOptions options) {
        StringBuilder sb = new StringBuilder().append("[");
        for (String key : differences.keySet()) {
            sb.append("{\"path\":").append('"').append(key).append("\",");
            sb.append(differences.get(key).asJsonFragment(truncateBinary));
            sb.append("},");
        }
        int index = sb.lastIndexOf(",");
        if (index > 0) {
            sb.deleteCharAt(index);
        }
        sb.append("]");
        return sb.toString();
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String key : differences.keySet()) {
            sb.append(key).append(": ").append(differences.get(key)).append(" ");
        }
        return sb.toString();
    }
    
    public String getNamespace() {
        return key == null ? null : key.namespace; 
    }

    public String getSetName() {
        return key == null ? null : key.setName; 
    }
    
    public int getCluster1Index() {
        return cluster1Index;
    }
    
    public int getCluster2Index() {
        return cluster2Index;
    }
}
package com.aerospike.comparator;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import com.aerospike.comparator.RecordComparator.DifferenceType;

public class DifferenceSet {
    private final Deque<String> pathParts = new ArrayDeque<>();
    private final Map<String, DifferenceValue> differences = new HashMap<>();
    private final boolean quickCompare;
    
    public DifferenceSet() {
        this(false);
    }
    
    public DifferenceSet(boolean quickCompare) {
        this.quickCompare = quickCompare;
    }
    
    protected void pushPath(String pathPart) {
        this.pathParts.add(pathPart);
    }

    protected String popPath() {
        return this.pathParts.pop();
    }

    protected String getCurrentPath() {
        return String.join("/", pathParts);
    }

    public void addDifference(String path, DifferenceType difference, Object obj1, Object obj2) {
        String fullPath = pathParts.size() == 0 ? path : this.getCurrentPath() + "/" + path;
        this.differences.put(fullPath, new DifferenceValue(difference, obj1, obj2));
    }

    public void addDifference(String path, DifferenceType difference, Object obj1, Object obj2, int index) {
        String fullPath = pathParts.size() == 0 ? path : this.getCurrentPath() + "/" + path;
        this.differences.put(fullPath, new DifferenceValue(difference, obj1, obj2, index));
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
    
    public String getAsJson() {
        StringBuilder sb = new StringBuilder().append("[");
        for (String key : differences.keySet()) {
            sb.append("{\"path\":").append('"').append(key).append("\",").append(differences.get(key).asJsonFragment()).append("},");
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
            sb.append(key).append(": ").append(differences.get(key)).append("\n");
        }
        return sb.toString();
    }
}
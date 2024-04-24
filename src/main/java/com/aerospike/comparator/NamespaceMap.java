package com.aerospike.comparator;

public class NamespaceMap {
    private String name;
    private String clusterName;
    private int clusterIndex;
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getClusterName() {
        return clusterName;
    }
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }
    public int getClusterIndex() {
        return clusterIndex;
    }
    public void setClusterIndex(int clusterIndex) {
        this.clusterIndex = clusterIndex;
    }
}

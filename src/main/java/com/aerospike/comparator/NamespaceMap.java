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
    /**
     * Return the cluster index as specified in the config file. This should be in the range 1 <= clusterIndex <= number of clusters
     * @return
     */
    public int getClusterIndex() {
        return clusterIndex;
    }
    /**
     * Set the cluster index as specified in the config file. This should be in the range 1 <= clusterIndex <= number of clusters
     * @return
     */
    public void setClusterIndex(int clusterIndex) {
        this.clusterIndex = clusterIndex;
    }
    
    /**
     * Get the cluster index in the range 0 <= index < numberOfClusters
     * @return
     */
    public int getInternalClusterIndex() {
        return this.clusterIndex-1;
    }
    @Override
    public String toString() {
        return String.format(" as %s on cluster[%s/%d]", this.name, this.clusterName, this.clusterIndex);
    }
}

package com.aerospike.comparator;

import java.util.List;

public class ConfigOptions {
    private List<ClusterConfig> clusters;
    private List<NamespaceMapping> namespaceMapping;
    private NetworkOptions network;

    public List<ClusterConfig> getClusters() {
        return clusters;
    }

    public void setClusters(List<ClusterConfig> clusters) {
        this.clusters = clusters;
    }

    public List<NamespaceMapping> getNamespaceMapping() {
        return namespaceMapping;
    }

    public void setNamespaceMapping(List<NamespaceMapping> namespaceMapping) {
        this.namespaceMapping = namespaceMapping;
    }
    
    public NetworkOptions getNetwork() {
        return network;
    }
    
    public void setNetwork(NetworkOptions network) {
        this.network = network;
    }
    
    @Override
    public String toString() {
        return String.format("ClusterConfigs: %s\nNamespace Mappings: %s", this.clusters, this.namespaceMapping);
    }
    
    /**
     * Map any cluster names to cluster ordinals and detect any errors.
     * @param resolver
     * @return null if there are no errors, otherwise return the validation error
     */
    public String resolveNamespaceMappingClusterNamesAndValidate(ClusterNameResolver resolver) {
        if (namespaceMapping != null) {
            for (int i = 0; i < this.namespaceMapping.size(); i++) {
                NamespaceMapping thisMapping = this.namespaceMapping.get(i);
                String result = thisMapping.resolveNamespaceMappingClusterNamesAndValidate(resolver, i);
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }
    
    public String getNamespace(String name, int clusterOrdinal) {
        if (namespaceMapping != null) {
            for (NamespaceMapping thisMapping : this.namespaceMapping) {
                if (thisMapping.getNamespace().equals(name)) {
                    for (NamespaceMap thisMap : thisMapping.getMappings()) {
                        if (thisMap.getInternalClusterIndex() == clusterOrdinal) {
                            return thisMap.getName();
                        }
                    }
                }
            }
        }
        return name;
    }
    
    /** 
     * Perform a reverse namespace mapping. So if namespace A is mapped to namespace B on cluster 2, and 
     * namespace B is passed, A will be returned.
     * <p/>
     * NOTE: This method performs a "best guess" for this resolution as there is no guarantee that multiple
     * namespaces do not map to the same namespace. Eg A->B, C->B, there is no way of ensuring the correct
     * cluster is returned
     * @param name
     * @return
     */
    public String findSourceNamespaceFor(String name) {
        if (namespaceMapping != null) {
            for (NamespaceMapping thisMapping : this.namespaceMapping) {
                List<NamespaceMap> mappings = thisMapping.getMappings();
                for (NamespaceMap thisMap : mappings) {
                    if (name.equals(thisMap.getName())) {
                        return thisMapping.getNamespace();
                    }
                }
            }
        }
        return name;
    }

    public boolean isNamespaceNameOverridden(String name) {
        if (namespaceMapping != null) {
            for (NamespaceMapping thisMapping : this.namespaceMapping) {
                if (thisMapping.getNamespace().equals(name)) {
                    for (NamespaceMap thisMap : thisMapping.getMappings()) {
                        if (!name.equals(thisMap.getName())) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }
}

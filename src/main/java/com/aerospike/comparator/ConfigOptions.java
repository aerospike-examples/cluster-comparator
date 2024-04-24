package com.aerospike.comparator;

import java.util.List;

public class ConfigOptions {
    private List<ClusterConfig> clusters;
    private List<NamespaceMapping> namespaceMapping;

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
                        if (thisMap.getClusterIndex() == clusterOrdinal+1) {
                            return thisMap.getName();
                        }
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

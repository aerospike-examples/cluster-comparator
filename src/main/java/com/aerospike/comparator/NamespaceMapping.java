package com.aerospike.comparator;

import java.util.List;

public class NamespaceMapping {
    private String namespace;
    private List<NamespaceMap> mappings;

    public String getNamespace() {
        return namespace;
    }
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
    public List<NamespaceMap> getMappings() {
        return mappings;
    }
    public void setMappings(List<NamespaceMap> mappings) {
        this.mappings = mappings;
    }
    
    /**
     * Resolved any clusters names into cluster ordinals and validate for errors.
     * @param resolver
     * @return null if the mappings are valid, an error string otherwise
     */
    public String resolveNamespaceMappingClusterNamesAndValidate(ClusterNameResolver resolver, int index) {
        if (namespace == null || namespace.isEmpty()) {
            return String.format("Namespace mapping %d is missing the name of the namespace", index);
        }
        else {
            for (int i = 0; i < this.mappings.size(); i++) {
                NamespaceMap thisMap = this.mappings.get(i);
                if (thisMap.getName() == null || thisMap.getName().isEmpty()) {
                    return String.format("Mapping %d for namespace '%s' is missing the destination cluster namespace name", i, namespace);
                }
                if (thisMap.getClusterName() != null) {
                    int id = resolver.clusterNameToId(thisMap.getClusterName());
                    if (id < 0) {
                        return String.format("Mapping for namespace '%s' has a cluster name of '%s' which is not a known name.", 
                                namespace, thisMap.getClusterName());
                    }
                    if (thisMap.getClusterIndex() > 0 && (id+1) != thisMap.getClusterIndex()) {
                        return String.format("Mapping for namespace '%s' has a cluster name of '%s' but an index of %d which do not match.", 
                                namespace, thisMap.getClusterName(), thisMap.getClusterIndex());
                    }
                    else {
                        thisMap.setClusterIndex(id+1);
                    }
                }
                else if (thisMap.getClusterIndex() > 0) {
                    if (!resolver.isClusterIdValid(thisMap.getClusterIndex())) {
                        return String.format("Mapping for namespace '%s' has a cluster index of %d which is greater than the number of clusters",
                                namespace, thisMap.getClusterIndex());
                    }
                }
            }
        }
        return null;
    }
}

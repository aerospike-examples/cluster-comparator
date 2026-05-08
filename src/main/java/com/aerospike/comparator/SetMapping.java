package com.aerospike.comparator;

import java.util.List;

public class SetMapping {
    private String set;
    private List<SetMap> mappings;

    public String getSet() {
        return set;
    }
    public void setSet(String set) {
        this.set = set;
    }
    public List<SetMap> getMappings() {
        return mappings;
    }
    public void setMappings(List<SetMap> mappings) {
        this.mappings = mappings;
    }

    /**
     * Resolve any cluster names into cluster ordinals and validate for errors.
     * @return null if the mappings are valid, an error string otherwise
     */
    public String resolveSetMappingClusterNamesAndValidate(ClusterNameResolver resolver, int index) {
        if (set == null || set.isEmpty()) {
            return String.format("Set mapping %d is missing the name of the set", index);
        }
        for (int i = 0; i < this.mappings.size(); i++) {
            SetMap thisMap = this.mappings.get(i);
            if (thisMap.getName() == null || thisMap.getName().isEmpty()) {
                return String.format("Mapping %d for set '%s' is missing the destination cluster set name", i, set);
            }
            if (thisMap.getClusterName() != null) {
                int id = resolver.clusterNameToId(thisMap.getClusterName());
                if (id < 0) {
                    return String.format("Mapping for set '%s' has a cluster name of '%s' which is not a known name.",
                            set, thisMap.getClusterName());
                }
                if (thisMap.getClusterIndex() > 0 && (id + 1) != thisMap.getClusterIndex()) {
                    return String.format("Mapping for set '%s' has a cluster name of '%s' but an index of %d which do not match.",
                            set, thisMap.getClusterName(), thisMap.getClusterIndex());
                }
                else {
                    thisMap.setClusterIndex(id + 1);
                }
            }
            else if (thisMap.getClusterIndex() > 0) {
                if (!resolver.isClusterIdValid(thisMap.getInternalClusterIndex())) {
                    return String.format("Mapping for set '%s' has a cluster index of %d which is outside the range of [0, %d)",
                            set, thisMap.getClusterIndex(), resolver.getNumberOfClusters());
                }
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return String.format("%s->%s", this.set, this.mappings);
    }
}

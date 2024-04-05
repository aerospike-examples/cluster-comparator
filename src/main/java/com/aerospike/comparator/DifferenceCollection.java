package com.aerospike.comparator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.aerospike.client.Key;
import com.aerospike.comparator.RecordComparator.DifferenceType;

public class DifferenceCollection {
    private List<DifferenceSet> differenceSets = null;
    private boolean isQuickCompare = false;
    private final List<Integer> clustersWithRecord;
    
    public static class RecordDifferences {
        private List<BinDifferences> binDiffs = null;

        public void addBinDifferences(BinDifferences diffs) {
            if (diffs != null) {
                if (binDiffs == null) {
                    binDiffs = new ArrayList<>();
                }
                binDiffs.add(diffs);
            }
        }
        public String toHumanString(ClusterNameResolver resolver) {
            if (binDiffs == null) {
                return "";
            }
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (BinDifferences theseDiffs : binDiffs) {
                if (!first) {
                    sb.append(", ");
                }
                first = false;
                sb.append(theseDiffs.toHumanString(resolver));
            }
            return sb.toString();
        }
        public String toRawString(ClusterNameResolver resolver) {
            if (binDiffs == null) {
                return "";
            }
            StringBuilder sb = new StringBuilder().append('{');
            boolean first = true;
            for (BinDifferences theseDiffs : binDiffs) {
                if (!first) {
                    sb.append(", ");
                }
                first = false;
                sb.append(theseDiffs.toRawString(resolver));
            }
            return sb.append('}').toString();
        }
    }
    
    private static class BinDifferences {
        private final String binName;
        private Set<Integer> clustersMissing;
        private Set<Set<Integer>> clustersWithSameValues;
        
        public BinDifferences(String binName) {
            this.binName = binName;
        }
        public void addMissingCluster(int clusterId) {
            if (clustersMissing == null) {
                clustersMissing = new HashSet<>();
            }
            clustersMissing.add(clusterId);
        }

        public String getBinName() {
            return binName;
        }
        
        public Set<Integer> getClustersMissing() {
            return clustersMissing;
        }
        public void setClustersMissing(Set<Integer> clustersMissing) {
            this.clustersMissing = clustersMissing;
        }
        public Set<Set<Integer>> getClustersWithSameValues() {
            return clustersWithSameValues;
        }
        public void setClustersWithSameValues(Set<Set<Integer>> clustersWithSameValues) {
            this.clustersWithSameValues = clustersWithSameValues;
        }
        
        public boolean hasClustersMissingBin() {
            return clustersMissing != null;
        }
        
        public boolean hasDifferentValues() {
            return this.clustersWithSameValues != null;
        }
        
        private void getClusterNames(Set<Integer> set, StringBuilder sb, ClusterNameResolver resolver) {
            List<Integer> list = new ArrayList<>(set);
            Collections.sort(list);
            sb.append('[');
            boolean first = true;
            for (Integer i : list) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                sb.append(resolver.clusterIdToName(i));
            }
            sb.append(']');
        }

        public String toHumanString(ClusterNameResolver resolver) {
            StringBuilder sb = new StringBuilder().append("Bin \"").append(binName).append("\"");
            if (this.hasClustersMissingBin()) {
                sb.append("is missing on clusters ");
                getClusterNames(this.getClustersMissing(), sb, resolver);
            }
            if (this.hasDifferentValues()) {
                Set<Set<Integer>> setList = this.getClustersWithSameValues();
                boolean first = true;
                sb.append(" has different values ");
                for (Set<Integer> set : setList) {
                    if (!first) {
                        sb.append(" vs ");
                    }
                    first = false;
                    getClusterNames(set, sb, resolver);
                }
            }
            return sb.toString();
        }
        
        public String toRawString(ClusterNameResolver resolver) {
            StringBuilder sb = new StringBuilder().append('"').append(binName).append("\":{");
            boolean hasMissing = false;
            if (this.hasClustersMissingBin()) {
                sb.append("\"missing\": ");
                getClusterNames(this.getClustersMissing(), sb, resolver);
                hasMissing = true;
            }
            if (this.hasDifferentValues()) {
                if (hasMissing) {
                    sb.append(',');
                }
                Set<Set<Integer>> setList = this.getClustersWithSameValues();
                boolean first = true;
                sb.append("\"differs\": [");
                for (Set<Integer> set : setList) {
                    if (!first) {
                        sb.append(",");
                    }
                    first = false;
                    getClusterNames(set, sb, resolver);
                }
                sb.append(']');
            }
            return sb.append('}').toString();
        }
    }
    
    public DifferenceCollection(List<Integer> clustersWithRecord) {
        this.clustersWithRecord = clustersWithRecord;
    }
    
    public void add(DifferenceSet differenceSet)  {
        if (differenceSet != null && differenceSet.areDifferent()) {
            if (differenceSets == null) {
                differenceSets = new ArrayList<>();
             }
            this.differenceSets.add(differenceSet);
            this.isQuickCompare = differenceSet.isQuickCompare();
        }
    }
    
    public boolean hasDifferences() {
        return differenceSets != null;
    }
    
    public boolean isQuickCompare() {
        return isQuickCompare;
    }
    
    public List<DifferenceSet> getDifferenceSets() {
        return this.differenceSets;
    }

    /**
     * Return the bin name from the path. Note that paths do not start with a slash and if the path
     * only goes to the bin name may not end with a slash.
     * @param path
     * @return
     */
    private String getBinFromPath(String path) {
        int index = 0;
        for (int i = 0; i < 2; i++) {
            index = path.indexOf("/", index)+1;
            if (index == 0) {
                return path;
            }
        }
        int endIndex = path.indexOf("/", index);
        if (endIndex < 0) {
            return path.substring(index);
        }
        else {
            return path.substring(index, endIndex);
        }
    }
    
    private void addSameValues(Set<Set<Integer>> clustersWithSameValues, int same1, int same2) {
        Set<Integer> firstFound = null;
        for (Iterator<Set<Integer>> iterator = clustersWithSameValues.iterator(); iterator.hasNext();) {
            Set<Integer> thisSameValue = iterator.next();
            if (thisSameValue.contains(same1) || thisSameValue.contains(same2)) {
                if (firstFound == null) {
                    // This is the first match, add in both same values
                    firstFound = thisSameValue;
                    thisSameValue.add(same1);
                    thisSameValue.add(same2);
                }
                else {
                    // There is already a set which contains both of these values, so we need to 
                    // merge this set into the other set and remove this set. For example, there
                    // may be one set with [A,B,C] and another with [C,D], we want to end up with just [A,B,C,D]
                    firstFound.addAll(thisSameValue);
                    iterator.remove();
                }
            }
        }
        if (firstFound == null) {
            // This is a new match
            Set<Integer> thisMatch = new HashSet<>();
            thisMatch.add(same1);
            thisMatch.add(same2);
            clustersWithSameValues.add(thisMatch);
        }
    }
    /**
     * Determine which clusters have the same values. So if there are 5 clusters, A,B,C,D,E, we might have 2 sets
     * of discrete values: A,B and C,D,E. In this case the result would be Set(Set(A,B), Set(C,D,E))
     * @param clustersWithDifferentValues
     * @param clusterCount
     * @return
     */
    private Set<Set<Integer>> getClustersWithSameValue(Set<String> clustersWithDifferentValues, Set<Integer> clustersMissingBin) {
        Set<Set<Integer>> results = new HashSet<>();
        // Prepopulate with all clusters, to assume they all have different values
        for (int i = 0; i < clustersWithRecord.size(); i++) {
            if (clustersMissingBin == null || !clustersMissingBin.contains(i)) {
                Set<Integer> thisSet = new HashSet<>();
                thisSet.add(clustersWithRecord.get(i));
                results.add(thisSet);
            }
        }
        for (int i = 0; i < clustersWithRecord.size(); i++) {
            if (clustersMissingBin != null && clustersMissingBin.contains(i)) {
                continue;
            }
            for (int j = i+1; j < clustersWithRecord.size(); j++) {
                if (clustersMissingBin != null && clustersMissingBin.contains(j)) {
                    continue;
                }
                String cluster = clustersWithRecord.get(i)+"~"+clustersWithRecord.get(j);
                if (!clustersWithDifferentValues.contains(cluster)) {
                    addSameValues(results, i, j);
                }
            }
        }
        return results;
    }
    
    private BinDifferences getBinDiffs(String binName, List<DifferenceValue> diffs) {
        BinDifferences result = null;
        
        Set<String> clustersWithDifferentValues = null;
        for (DifferenceValue diff : diffs) {
            DifferenceType type = diff.getType();
            switch (type) {
            case CONTENTS:
                if (clustersWithDifferentValues == null) {
                    clustersWithDifferentValues = new HashSet<>();
                }
                clustersWithDifferentValues.add(diff.getCluster1()+"~"+diff.getCluster2());
                clustersWithDifferentValues.add(diff.getCluster2()+"~"+diff.getCluster1());
                break;
            case ONLY_ON_1:
                if (result == null) {
                    result = new BinDifferences(binName);
                }
                result.addMissingCluster(diff.getCluster2());
                break;
            case ONLY_ON_2:
                if (result == null) {
                    result = new BinDifferences(binName);
                }
                result.addMissingCluster(diff.getCluster1());
                break;
            }
        }
        if (clustersWithDifferentValues != null) {
            if (result == null) {
                result = new BinDifferences(binName);
            }
            result.setClustersWithSameValues(getClustersWithSameValue(clustersWithDifferentValues, result.getClustersMissing()));
        }
        return result;
    }
    
    public RecordDifferences getBinsDifferent(Key key) {
        RecordDifferences result = null;
        Map<String, List<DifferenceValue>> differencesByBin = new HashMap<>();
        for (DifferenceSet thisSet : differenceSets) {
            Map<String, DifferenceValue> differences = thisSet.getDifferences();
            for (String path : differences.keySet()) {
                String binName = getBinFromPath(path);
                DifferenceValue diffValue = differences.get(path);
                List<DifferenceValue> diffsForThisBin = differencesByBin.get(binName);
                if (diffsForThisBin == null) {
                    diffsForThisBin = new ArrayList<>();
                    differencesByBin.put(binName, diffsForThisBin);
                }
                diffsForThisBin.add(diffValue);
            }
        }
        for (String binName : differencesByBin.keySet()) {
            BinDifferences theseDifferences = getBinDiffs(binName, differencesByBin.get(binName));
            if (theseDifferences != null) {
                if (result == null) {
                    result = new RecordDifferences();
                }
                result.addBinDifferences(theseDifferences);
            }
        }
        return result;
    }
}

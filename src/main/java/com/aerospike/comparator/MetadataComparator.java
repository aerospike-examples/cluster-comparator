package com.aerospike.comparator;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.cluster.Node;
import com.aerospike.comparator.RecordComparator.DifferenceType;
import com.aerospike.comparator.metadata.InfoParser;
import com.aerospike.comparator.metadata.MismatchingOnCluster;

public class MetadataComparator {
    private final ClusterComparatorOptions options;
    private final InfoParser infoParser;
    
    public MetadataComparator(final ClusterComparatorOptions options) {
        this.options = options;
        this.infoParser = new InfoParser();
    }
    
    private void compareNamespaces(IAerospikeClient client1, IAerospikeClient client2, DifferenceSet differenceSet) {
        Set<String> namespaces1 = this.infoParser.invokeCommandReturningSet(client1, "namespaces");
        Set<String> namespaces2 = this.infoParser.invokeCommandReturningSet(client2, "namespaces");
        
        for (String nsName : namespaces1) {
            if (namespaces2.contains(nsName)) {
                differenceSet.pushPath("namespace", nsName);
                List<Map<String, String>> nsDifferences1 = this.infoParser.invokeCommandReturningObjectOnAllNodes(client1, "namespace/" + nsName);
                List<Map<String, String>> nsDifferences2 = this.infoParser.invokeCommandReturningObjectOnAllNodes(client2, "namespace/" + nsName);
                removeMismatchesBetweenNodes(nsDifferences1, differenceSet, client1.getNodes(), true);
                removeMismatchesBetweenNodes(nsDifferences2, differenceSet, client2.getNodes(), false);
                RecordComparator comparator = new RecordComparator();
                comparator.compare(nsDifferences1.get(0), nsDifferences2.get(0), differenceSet);
                differenceSet.popPath(2);
                namespaces2.remove(nsName);
            }
            else {
                differenceSet.addDifference(nsName, DifferenceType.ONLY_ON_1, nsName, null);
            }
        }
        for (String nsName : namespaces2) {
            differenceSet.addDifference(nsName, DifferenceType.ONLY_ON_2, null, nsName);
        }
    }
    
    private void compareSets(IAerospikeClient client1, IAerospikeClient client2, DifferenceSet differenceSet) {
        // TODO: Handle set differences across nodes
        List<Map<String, String>> sets1 = this.infoParser.invokeCommandReturningObjectList(client1, "sets");
        List<Map<String, String>> sets2 = this.infoParser.invokeCommandReturningObjectList(client2, "sets");

        for (Map<String, String> set1Details : sets1) {
            String nsName = set1Details.get("ns");
            String setName = set1Details.get("set");
            Optional<Map<String, String>> set2DetailsOpt = sets2.stream().filter(
                    set -> set.get("set").equals(setName) && set.get("ns").equals(nsName)
                ).findFirst();
            
            if (set2DetailsOpt.isPresent()) {
                Map<String, String> set2Details = set2DetailsOpt.get();
                differenceSet.pushPath("namespace", nsName, "set", setName);
                RecordComparator comparator = new RecordComparator();
                comparator.compare(set1Details, set2Details, differenceSet);
                differenceSet.popPath(4);
                sets2.remove(set2Details);
            }
            else {
                differenceSet.pushPath("namespace", nsName, "set");
                differenceSet.addDifference(setName, DifferenceType.ONLY_ON_1, setName, null);
                differenceSet.popPath(3);
            }
        }
        for (Map<String, String> set2Details : sets2) {
            differenceSet.pushPath("namespace", set2Details.get("ns"), "set");
            differenceSet.addDifference(set2Details.get("set"), DifferenceType.ONLY_ON_2, set2Details.get("set"), null);
            differenceSet.popPath(3);
        }
    }
    
    private void removeMismatchesBetweenNodes(List<Map<String, String>> data, DifferenceSet differences, Node[] nodes, boolean isSide1) {
        if (data == null || data.isEmpty()) {
            return;
        }
        Set<String> keys = new HashSet<>();
        for (Map<String, String> thisData : data) {
            keys.addAll(thisData.keySet());
        }
        
        for (String key : keys) {
            String item0 = data.get(0).get(key);
            boolean allEqual = data.stream().allMatch(dataItem -> item0.equals(dataItem.get(key)));
            if (!allEqual) {
                String mismatchString = new MismatchingOnCluster(key).addMismatchingValues(data, nodes).toString();
                differences.addDifference(key, DifferenceType.CONTENTS, isSide1?mismatchString : "", isSide1?"":mismatchString);
                for (Map<String, String> thisMap : data) {
                    thisMap.remove(key);
                }
            }
        }
    }
    
    public void compareStanzas(IAerospikeClient client1, IAerospikeClient client2, String stanza, DifferenceSet differenceSet) {
        List<Map<String, String>> client1Config = this.infoParser.invokeCommandReturningObjectOnAllNodes(client1, "get-config:context="+stanza);
        List<Map<String, String>> client2Config = this.infoParser.invokeCommandReturningObjectOnAllNodes(client2, "get-config:context="+stanza);
        differenceSet.pushPath(stanza);
        removeMismatchesBetweenNodes(client1Config, differenceSet, client1.getNodes(), true);
        removeMismatchesBetweenNodes(client2Config, differenceSet, client2.getNodes(), false);
        RecordComparator comparator = new RecordComparator();
        comparator.compare(client1Config.get(0), client2Config.get(0), differenceSet);
        differenceSet.popPath();
    }
    
    public DifferenceSet compareMetaData(IAerospikeClient client1, IAerospikeClient client2) {
        DifferenceSet differenceSet = new DifferenceSet(new Key("config", "", ""), false, options.getPathOptions());
        differenceSet.pushPath("/config");
        compareStanzas(client1, client2, "network", differenceSet);
        compareStanzas(client1, client2, "security", differenceSet);
        compareStanzas(client1, client2, "service", differenceSet);
        compareStanzas(client1, client2, "network", differenceSet);
        
        compareNamespaces(client1, client2, differenceSet);
        compareSets(client1, client2, differenceSet);
//        compareSindex(client1, client2, differenceSet);
        
        differenceSet.popPath();
        
        System.out.println(differenceSet);
        return differenceSet;
    }
}

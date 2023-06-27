package com.aerospike.comparator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
    /** Items (besides metrics) which should be removed from the namespace comparison */
    private final static List<String> NS_FILTER_OUT = Arrays.asList(new String[] {"objects", "tombstones", "truncating"});
    /** Items (besides metrics) which should be removed from the set comparison */
    private final static List<String> SET_FILTER_OUT = Arrays.asList(new String[] {"objects", "tombstones", "truncating"});
    
    public MetadataComparator(final ClusterComparatorOptions options) {
        this.options = options;
        this.infoParser = new InfoParser();
    }
    
    private void filterOut(List<Map<String, String>> items, List<String> keys) {
        for (Map<String, String> thisItem : items) {
            for (String key : keys) {
                thisItem.remove(key);
            }
        }
    }
    
    private void compareNamespaces(IAerospikeClient client1, IAerospikeClient client2, DifferenceSet differenceSet) {
        Set<String> namespaces1 = this.infoParser.invokeCommandReturningSetOnAllNodes(client1, "namespaces");
        Set<String> namespaces2 = this.infoParser.invokeCommandReturningSetOnAllNodes(client2, "namespaces");
        
        for (String nsName : namespaces1) {
            if (namespaces2.contains(nsName)) {
                differenceSet.pushPath("namespace", nsName);
                List<Map<String, String>> nsDifferences1 = this.infoParser.invokeCommandReturningObjectOnAllNodes(client1, "namespace/" + nsName);
                List<Map<String, String>> nsDifferences2 = this.infoParser.invokeCommandReturningObjectOnAllNodes(client2, "namespace/" + nsName);
                this.filterOut(nsDifferences1, NS_FILTER_OUT);
                this.filterOut(nsDifferences1, NS_FILTER_OUT);
                this.removeMismatchesBetweenNodes(nsDifferences1, differenceSet, client1.getNodes(), Side.SIDE_1);
                this.removeMismatchesBetweenNodes(nsDifferences2, differenceSet, client2.getNodes(), Side.SIDE_2);
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
    
    /**
     * The objects need to be transposed. For example. we might have a: List(Per-Node) contains List(Per-Set) of Set data.
     * What we really want is List(Per-Set) contains List(Per-Node) of Set-data. 
     * <p>
     * Note that there is no guarantee that all nodes will contain all sets so we must handle this case.
     * Each object must have an identifying name so they can be grouped together, hence the name finder.
     * @param orignal
     * @return
     */
    private List<List<Map<String, String>>> transpose(List<List<Map<String, String>>> orignal, NameFinder nameFinder) {
        List<List<Map<String, String>>> results = new ArrayList<>();
        Map<String, List<Map<String, String>>> objects = new HashMap<>();
        return results;
    }
    private void compareSets(IAerospikeClient client1, IAerospikeClient client2, DifferenceSet differenceSet) {
        List<List<Map<String, String>>> sets1 = this.infoParser.invokeCommandReturningObjectListOnAllNodes(client1, "sets");
        List<List<Map<String, String>>> sets2 = this.infoParser.invokeCommandReturningObjectListOnAllNodes(client2, "sets");
/*
        this.filterOut(sets1, SET_FILTER_OUT);
        this.filterOut(sets2, SET_FILTER_OUT);;
        this.removeMismatchesBetweenNodes(sets1, differenceSet, client1.getNodes(), Side.SIDE_1);
        this.removeMismatchesBetweenNodes(sets2, differenceSet, client2.getNodes(), Side.SIDE_2);
        
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
        */
    }
    
    private void removeMismatchesBetweenNodes(List<Map<String, String>> data, DifferenceSet differences, Node[] nodes, Side side) {
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
                String mismatchString = new MismatchingOnCluster(key).addMismatchingValues(data, nodes, side).toString();
                differences.addDifference(key, DifferenceType.CONTENTS, side == Side.SIDE_1 ? mismatchString : "", side == Side.SIDE_2 ? mismatchString : "");
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
        removeMismatchesBetweenNodes(client1Config, differenceSet, client1.getNodes(), Side.SIDE_1);
        removeMismatchesBetweenNodes(client2Config, differenceSet, client2.getNodes(), Side.SIDE_2);
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

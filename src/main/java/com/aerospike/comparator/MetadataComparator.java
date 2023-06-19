package com.aerospike.comparator;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.comparator.RecordComparator.DifferenceType;
import com.aerospike.comparator.metadata.InfoParser;

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
                Map<String, String> nsDifferences1 = this.infoParser.invokeCommandReturningObject(client1, "namespace/" + nsName);
                Map<String, String> nsDifferences2 = this.infoParser.invokeCommandReturningObject(client2, "namespace/" + nsName);
                RecordComparator comparator = new RecordComparator();
                comparator.compare(nsDifferences1, nsDifferences2, differenceSet);
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
    
    public DifferenceSet compareMetaData(IAerospikeClient client1, IAerospikeClient client2) {
        Map<String, String> client1Config = this.infoParser.invokeCommandReturningObject(client1, "get-config");
        Map<String, String> client2Config = this.infoParser.invokeCommandReturningObject(client2, "get-config");
        
        RecordComparator comparator = new RecordComparator();
        DifferenceSet differenceSet = new DifferenceSet(new Key("config", "", 1), false, options.getPathOptions());
        differenceSet.pushPath("/config");
        comparator.compare(client1Config, client2Config, differenceSet);
        
        compareNamespaces(client1, client2, differenceSet);
        compareSets(client1, client2, differenceSet);
//        compareSindex(client1, client2, differenceSet);
        
        differenceSet.popPath();
        
        System.out.println(differenceSet);
        return differenceSet;
    }
}

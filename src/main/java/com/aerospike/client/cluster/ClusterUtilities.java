package com.aerospike.client.cluster;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.command.Buffer;

/**
 * This class can be used to perform "interesting" operations on the cluster, such as determining the nodes in the cluster, the partition map, etc.
 * <p/>
 * @author timfaulkes
 *
 */
public class ClusterUtilities {
    private final Cluster cluster;
    private final IAerospikeClient client;
    
    public ClusterUtilities(IAerospikeClient client) {
        this.client = client;
        this.cluster = client.getCluster();
    }
    
    public int getPartitionForKey(Key key) {
        return (Buffer.littleBytesToInt(key.digest, 0) & 0xFFFF) % Node.PARTITIONS;
    }
    
    public Node[] findAllNodesForKey(Key key) {
        return findAllNodesForPartition(key.namespace, getPartitionForKey(key));
    }
    
    public Node[] findAllNodesForPartition(String namespace, int partId) {
        if (cluster == null) {
            throw new IllegalArgumentException("findAllNodesForPartition cannot be called if there is no cluster information");
        }
        Map<String, Partitions> partitionMap = this.cluster.partitionMap;
        Partitions partitions = partitionMap.get(namespace);
        if (partitions == null) {
            throw new IllegalArgumentException("Namespace " + namespace + " does not exist");
        }
        Node[] nodes = new Node[partitions.replicas.length];
        AtomicReferenceArray<Node>[] replicas = partitions.replicas;
        for (int replicaId = 0; replicaId < replicas.length; replicaId++) {
            nodes[replicaId] = replicas[replicaId].get(partId);
        }
        return nodes;
    }
    
    public int[] getPartitionRegimes(String namespace) {
        return this.cluster.partitionMap.get(namespace).regimes.clone();
    }
    
    public String findKeyOnSpecificNodes(Node master, Node replica, String namespace, String set) {
        if (cluster == null) {
            throw new IllegalArgumentException("findKeyOnSpecificNodes cannot be called if there is no cluster information");
        }
        Map<String, Partitions> partitionMap = this.cluster.partitionMap;
        Partitions partitions = partitionMap.get(namespace);
        if (partitions == null) {
            throw new IllegalArgumentException("Namespace " + namespace + " does not exist");
        }
        AtomicReferenceArray<Node>[] replicas = partitions.replicas;

        Set<Integer> partitionsOnMaster = new HashSet<>();
        Set<Integer> partitionsOnReplica = new HashSet<>();
        
        if (replicas.length < 2) {
            return null;
        }
        for (int replicaId = 0; replicaId < replicas.length; replicaId++) {
            for (int partId = 0; partId < replicas[replicaId].length(); partId++) {
                Node node = replicas[replicaId].get(partId);
                if (node != null && node.equals(master) && replicaId == 0) {
                    partitionsOnMaster.add(partId);
                }
                else if (node != null && node.equals(replica) && replicaId == 1) {
                    partitionsOnReplica.add(partId);
                }
            }
        }
        if (partitionsOnReplica.isEmpty() || partitionsOnMaster.isEmpty()) {
            return null;
        }
        for (int i = 0; i < 100000; i++) {
            String keyName = "key" + i;
            Key key = new Key(namespace, set, keyName);
            int partitionId = getPartitionForKey(key);
            if (partitionsOnMaster.contains(partitionId) && partitionsOnReplica.contains(partitionId)) {
                return keyName;
            }
        }
        return null;
    }
    
    private void printPartMap(String namespace, int maxLength) {
        Map<String, Partitions> partitionMap = this.cluster.partitionMap;
        Partitions partitions = partitionMap.get(namespace);
        if (partitions == null) {
            return;
        }
        AtomicReferenceArray<Node>[] replicas = partitions.replicas;

        Map<String, Map<Integer, Set<Integer>>> nodeToReplicaToParts = new HashMap<>();
        for (int replicaId = 0; replicaId < replicas.length; replicaId++) {
            for (int partId = 0; partId < replicas[replicaId].length(); partId++) {
                Node node = replicas[replicaId].get(partId);
                
                Map<Integer, Set<Integer>> replicaToParts = nodeToReplicaToParts.get(node.getName());
                if (replicaToParts == null) {
                    replicaToParts = new HashMap<>();
                    nodeToReplicaToParts.put(node.getName(), replicaToParts);
                }
                Set<Integer> parts = replicaToParts.get(replicaId);
                if (parts == null) {
                    parts = new HashSet<>();
                    replicaToParts.put(replicaId, parts);
                }
                
                parts.add(partId);
            }
        }
        // Now iterate through each node and each replica
        Node[] nodes = this.cluster.getNodes();
        for (Node node : nodes) {
            System.out.printf("   Node: %s\n", node.getName());
            Map<Integer, Set<Integer>> replicaToParts = nodeToReplicaToParts.get(node.getName());
            if (replicaToParts == null) {
                System.out.printf("      No allocated partitions. This is unexpected.\n");
            }
            else {
                for (int i = 0; i < replicaToParts.size(); i++) {
                    System.out.printf("      Replica %d: ", i+1);
                    Set<Integer> parts = replicaToParts.get(i);
                    List<Integer> partList = new ArrayList<>(parts);
                    Collections.sort(partList);
                    StringBuilder buffer = new StringBuilder();
                    int lastOutput = -5;
                    int runCount = 0;
                    for (int k = 0; k < partList.size(); k++) {
                        int thisIndex = partList.get(k);
                        if (lastOutput+runCount == thisIndex) {
                            runCount++;
                        }
                        else {
                            if (runCount > 1) {
                                buffer.append("-").append(lastOutput+runCount-1).append(",");
                            }
                            else if (lastOutput >= 0) {
                                buffer.append(",");
                            }
                            lastOutput = thisIndex;
                            runCount = 1;
                            buffer.append(thisIndex);
                        }
                        if (maxLength > 0 && buffer.length() > maxLength ) {
                            break;
                        }
                    }
                    if (runCount > 1) {
                        buffer.append("-").append(lastOutput+runCount-1);
                    }
                    
                    if (maxLength > 0 && buffer.length() > maxLength) {
                        System.out.println(buffer.toString().substring(0, maxLength)+"...");
                    }
                    else {
                        System.out.println(buffer.toString());
                    }
                }
            }
        }
    }
    
    public void printInfo() {
        printInfo(false);
    }
    
    public void printInfo(boolean printPartMap) {
        printInfo(printPartMap, 0);
    }
    
    private String printRackInfo(Map<String, Integer> rackMap) {
        if (rackMap == null) {
            return "";
        }
        else {
            StringBuilder sb = new StringBuilder(1000);
            sb.append(" RackIds: ");
            for (String namespace : rackMap.keySet()) {
                sb.append(namespace).append("->").append(rackMap.get(namespace));
            }
            return sb.toString();
        }
    }
    public void printInfo(boolean printPartitionMap, int length) {
        System.out.println(cluster.clusterName == null ? "<unnamed cluster>" : cluster.clusterName);
        System.out.println("----------------------");
        System.out.printf("   Rack Aware: %b\n", cluster.rackAware);
        System.out.println();
        
        System.out.println("Nodes in cluster");
        System.out.println("----------------------");
        Node[] nodes = client.getNodes();
        for (Node node : nodes) {
            Map<String, Integer> rackMap = null;
            try {
                Field racksField = Node.class.getDeclaredField("racks");
                racksField.setAccessible(true);
                rackMap = (Map<String, Integer>) racksField.get(node);
            }
            catch (NoSuchFieldException | IllegalAccessException ignored) {
            }
            System.out.printf("Node: %s [%s] %s%s\n", node.getName(), node.getAddress().getAddress().toString(), node.getAddress().getHostName(), printRackInfo(rackMap));
        }
        System.out.println();

        HashMap<String, Partitions> partitionMap = this.cluster.partitionMap;
        for (String namespace : partitionMap.keySet()) {
            System.out.println(namespace);
            System.out.println("----------------------");
            Partitions theseParts = partitionMap.get(namespace);
            System.out.printf("   Strong Consistency: %b\n", theseParts.scMode);
            if (printPartitionMap) {
                printPartMap(namespace, length);
            }
            System.out.println();
        }
    }
}

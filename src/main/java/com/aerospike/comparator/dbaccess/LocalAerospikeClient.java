package com.aerospike.comparator.dbaccess;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.AerospikeException.InvalidNode;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;

public class LocalAerospikeClient implements AerospikeClientAccess {
    private final IAerospikeClient client;

    public LocalAerospikeClient(IAerospikeClient client) {
        super();
        this.client = client;
    }
    
    @Override
    public void close() {
        this.client.close();
    }
    
    @Override
    public void touch(WritePolicy policy, Key key) {
        this.client.touch(policy, key);
    }
    @Override
    public boolean exists(WritePolicy policy, Key key) {
        return this.client.exists(policy, key);
    }
    @Override
    public Record get(Policy policy, Key key) {
        return this.client.get(policy, key);
    }

    @Override
    public RecordSetAccess queryPartitions(QueryPolicy queryPolicy, Statement statement, PartitionFilter filter) {
        try {
            RecordSet recordSet = this.client.queryPartitions(queryPolicy, statement, filter);
            return new LocalRecordSet(recordSet);
        }
        catch (InvalidNode in) {
            RemoteUtils.handleInvalidNode(in, this.client);
            throw in;
        }
    }

    private Node[] getNodesAndValidate() {
        Node[] nodes = this.client.getNodes();
        if (nodes == null || nodes.length == 0) {
            throw new AerospikeException("No nodes listed in cluster, is Aerospike connected?");
        }
        return nodes;
    }
    
    @Override
    public Map<String, String> invokeInfoCommandOnAllNodes(String info) {
        Node[] nodes = getNodesAndValidate();
        Map<String, String> results = new HashMap<>();
        for (Node node : nodes) {
            results.put(node.getName(), Info.request(node, info));
        }
        return results;
    }

    @Override
    public String invokeInfoCommandOnANode(String info) {
        return Info.request(getNodesAndValidate()[0], info);
    }
    
    @Override
    public List<String> getNodeNames() {
        return Arrays.asList(client.getNodes()).stream().map(node -> node.getName()).collect(Collectors.toList());
    }

    @Override
    public boolean isLocal() {
        return true;
    }
}

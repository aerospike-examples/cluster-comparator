package com.aerospike.comparator.dbaccess;

import java.util.List;
import java.util.Map;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.Statement;

/**
 * Test double whose methods fail unless a test overrides the one it needs.
 */
public class StubAerospikeClient implements AerospikeClientAccess {
    @Override
    public boolean isLocal() {
        return true;
    }

    @Override
    public void close() {
    }

    @Override
    public void put(WritePolicy policy, Key key, Bin... bins) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void touch(WritePolicy policy, Key key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(WritePolicy policy, Key key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean exists(Policy policy, Key key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record get(Policy policy, Key key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean[] exists(BatchPolicy policy, Key[] keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record[] get(BatchPolicy policy, Key[] keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordMetadata getMetadata(WritePolicy policy, Key key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordSetAccess queryPartitions(QueryPolicy queryPolicy, Statement statement, PartitionFilter filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> invokeInfoCommandOnAllNodes(String info) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String invokeInfoCommandOnANode(String info) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getNodeNames() {
        throw new UnsupportedOperationException();
    }
}

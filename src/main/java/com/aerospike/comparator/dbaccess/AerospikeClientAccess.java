package com.aerospike.comparator.dbaccess;

import java.util.List;
import java.util.Map;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.Statement;

public interface AerospikeClientAccess {
    boolean isLocal();
    void close();
    void touch(WritePolicy policy, Key key);
    void delete(WritePolicy policy, Key key);
    boolean exists(Policy policy, Key key);
    Record get(Policy policy, Key key);
    RecordMetadata getMetadata(WritePolicy policy, Key key);
    RecordSetAccess queryPartitions(QueryPolicy queryPolicy, Statement statement, PartitionFilter filter);
    
    Map<String, String> invokeInfoCommandOnAllNodes(String info);
    String invokeInfoCommandOnANode(String info);
    List<String> getNodeNames();
}

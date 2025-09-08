package com.aerospike.comparator;

import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;

public class NetworkOptions {
    private NetworkSettings query;
    private NetworkSettings read;
    private NetworkSettings write;
    public NetworkSettings getQuery() {
        return query;
    }
    public void setQuery(NetworkSettings query) {
        this.query = query;
    }
    public NetworkSettings getRead() {
        return read;
    }
    public void setRead(NetworkSettings read) {
        this.read = read;
    }
    public NetworkSettings getWrite() {
        return write;
    }
    public void setWrite(NetworkSettings write) {
        this.write = write;
    }
    
    private <T extends Policy> T updatePolicy(T policy, NetworkSettings settings) {
        if (settings != null) {
            settings.applyTo(policy);
        }
        return policy;
    }
    
    public WritePolicy updateWritePolicy(WritePolicy policy) {
        return updatePolicy(policy, write);
    }
    public Policy updateReadPolicy(Policy policy) {
        return updatePolicy(policy, read);
    }
    public QueryPolicy updateQueryPolicy(QueryPolicy policy) {
        return updatePolicy(policy, query);
    }
}

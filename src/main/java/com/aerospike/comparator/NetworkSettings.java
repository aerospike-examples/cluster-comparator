package com.aerospike.comparator;

import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.Replica;

public class NetworkSettings {
    private Integer totalTimeout;
    private Integer socketTimeout;
    private Integer maxRetries;
    private Integer sleepBetweenRetries;
    private Replica replica;
    private Integer connectTimeout;
    
    public Integer getTotalTimeout() {
        return totalTimeout;
    }

    public void setTotalTimeout(Integer totalTimeout) {
        this.totalTimeout = totalTimeout;
    }

    public Integer getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(Integer socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public Integer getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(Integer maxRetries) {
        this.maxRetries = maxRetries;
    }

    public Integer getSleepBetweenRetries() {
        return sleepBetweenRetries;
    }

    public void setSleepBetweenRetries(Integer sleepBetweenRetries) {
        this.sleepBetweenRetries = sleepBetweenRetries;
    }

    public Replica getReplica() {
        return replica;
    }

    public void setReplica(Replica replica) {
        this.replica = replica;
    }

    public Integer getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Integer connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public <T extends Policy> T applyTo(T policy) {
        if (connectTimeout != null) {
            policy.connectTimeout = connectTimeout;
        }
        if (totalTimeout != null) {
            policy.totalTimeout = totalTimeout;
        }
        if (socketTimeout != null) {
            policy.socketTimeout = socketTimeout;
        }
        if (sleepBetweenRetries != null) {
            policy.sleepBetweenRetries = sleepBetweenRetries;
        }
        if (replica != null) {
            policy.replica = replica;
        }
        if (maxRetries != null) {
            policy.maxRetries = maxRetries;
        }
        return policy;
    }
}

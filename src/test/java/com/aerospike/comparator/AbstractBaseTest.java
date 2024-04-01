package com.aerospike.comparator;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;

public class AbstractBaseTest {
    public static final int NUM_CLUSTERS = 3;
    private final String[] hosts = new String[NUM_CLUSTERS];
    private final IAerospikeClient[] clients = new IAerospikeClient[NUM_CLUSTERS];
    public AbstractBaseTest() {
        hosts[0] = "172.17.0.2:3100";
        hosts[1] = "172.17.0.3:3101";
        hosts[2] = "172.17.0.4:3102";
        for (int i = 0; i < NUM_CLUSTERS; i++) {
            clients[i] = new AerospikeClient(null, Host.parseHosts(hosts[i], 3000));
        }
    }
    
    protected String getHostString(int clusterId) {
        if (clusterId < 0 || clusterId >= NUM_CLUSTERS) {
            throw new IllegalArgumentException(String.format("clusterId must be between 0 and %d, not %d", clients.length-1, clusterId));
        }
        return hosts[clusterId];
    }
    protected IAerospikeClient getClient(int clusterId) {
        if (clusterId < 0 || clusterId >= NUM_CLUSTERS) {
            throw new IllegalArgumentException(String.format("clusterId must be between 0 and %d, not %d", clients.length-1, clusterId));
        }
        return clients[clusterId];
    }
}

package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class ClusterTestHostsTest {

    @Test
    void defaultsToLocalIntegrationPorts() {
        assertEquals("localhost:3100", ClusterTestHosts.resolve(0));
        assertEquals("localhost:3101", ClusterTestHosts.resolve(1));
        assertEquals("localhost:3102", ClusterTestHosts.resolve(2));
    }
}

package com.aerospike.comparator.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.aerospike.comparator.web.ComparisonSession.State;

public class ComparisonSessionTest {

    @Test
    void invalidOptionsReturnAnErrorAndLeaveTheSessionIdle() {
        ComparisonSession session = new ComparisonSession();
        String error = session.start(new String[] {
                "--hosts1", "h1:3000",
                "--namespaces", "test",
                "--lookupBatchSize", "0"
        });
        assertNotNull(error);
        assertTrue(error.startsWith("Failed to initialize:"));
        assertEquals(State.IDLE, session.getState());
    }
}

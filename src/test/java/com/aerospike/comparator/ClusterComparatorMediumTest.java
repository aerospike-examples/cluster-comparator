package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.aerospike.client.Key;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.Statement;
import com.aerospike.comparator.dbaccess.AerospikeClientAccess;
import com.aerospike.comparator.dbaccess.RecordSetAccess;
import com.aerospike.comparator.dbaccess.StubAerospikeClient;

public class ClusterComparatorMediumTest {

    @Test
    void stopWithNoLimitDoesNotClaimALimitOfZero() throws Exception {
        ClusterComparator comparator = comparator("--quiet");
        // --quiet suppresses the summary. Rebuild without it.
        comparator = comparator();
        comparator.forceTerminate = true;
        String summary = summary(comparator);
        assertTrue(summary.contains("Comparison stopped before completion."));
        assertFalse(summary.contains("on a limit of 0"));
    }

    @Test
    void recordLimitIsReportedWhenTheMissingLimitIsZero() throws Exception {
        ClusterComparator comparator = comparator("--recordLimit", "5");
        comparator.forceTerminate = true;
        comparator.totalRecordsCompared.set(5);
        String summary = summary(comparator);
        assertTrue(summary.contains("Comparison terminated after comparing 5 records on a limit of 5"));
        assertFalse(summary.contains("on a limit of 0"));
    }

    @Test
    void missingLimitIsReportedWhenItWasActuallyReached() throws Exception {
        ClusterComparator comparator = comparator("--limit", "3");
        comparator.forceTerminate = true;
        Field missing = ClusterComparator.class.getDeclaredField("totalMissingRecords");
        missing.setAccessible(true);
        ((java.util.concurrent.atomic.AtomicLong) missing.get(comparator)).set(3);
        String summary = summary(comparator);
        assertTrue(summary.contains("Comparison terminated after finding 3 missing records on a limit of 3"));
    }

    @Test
    void workerRecordsAnErrorAndStopsTheRun() throws Exception {
        ClusterComparator comparator = comparator("--quiet");
        AerospikeClientAccess client = new StubAerospikeClient() {
            @Override
            public RecordSetAccess queryPartitions(QueryPolicy queryPolicy, Statement statement, PartitionFilter filter) {
                throw new IllegalAccessError("cached record");
            }
        };
        runWorker(comparator, client);
        assertTrue(comparator.forceTerminate);
        assertEquals(List.of(7), failedPartitions(comparator));
        assertEquals(List.of(8), partitionList(comparator));
    }

    @Test
    void workerRecordsAnExceptionAndContinues() throws Exception {
        ClusterComparator comparator = comparator("--quiet");
        AerospikeClientAccess client = new StubAerospikeClient() {
            @Override
            public RecordSetAccess queryPartitions(QueryPolicy queryPolicy, Statement statement, PartitionFilter filter) {
                throw new IllegalStateException("partition failed");
            }
        };
        runWorker(comparator, client);
        assertFalse(comparator.forceTerminate);
        assertEquals(List.of(7, 8), failedPartitions(comparator));
        assertTrue(partitionList(comparator).isEmpty());
    }

    @Test
    void mixedDeleteModesDoNotShareDurableDelete() throws Exception {
        ClusterComparator comparator = comparator(
                "--action", "scan_custom",
                "--customActions", "1:delete,2:durable_delete",
                "--skipChallenge",
                "--quiet");
        WritePolicy shared = writePolicy(comparator);
        RecordingClient client = new RecordingClient();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> expunge = pool.submit(() -> repeatDelete(comparator, client, 0, 40));
            Future<?> durable = pool.submit(() -> repeatDelete(comparator, client, 1, 40));
            expunge.get(5, TimeUnit.SECONDS);
            durable.get(5, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
        assertFalse(shared.durableDelete);
        assertEquals(80, client.policies.size());
        int expunges = 0;
        int durables = 0;
        for (WritePolicy policy : client.policies) {
            assertNotSame(shared, policy);
            if (policy.durableDelete) {
                durables++;
            } else {
                expunges++;
            }
        }
        assertEquals(40, expunges);
        assertEquals(40, durables);
    }

    @Test
    void failedDeleteChallengeStopsOtherWorkers() throws Exception {
        ClusterComparator comparator = comparator(
                "--action", "scan_custom",
                "--customActions", "1:delete,2:durable_delete",
                "--quiet");
        RecordingClient client = new RecordingClient();
        InputStream original = System.in;
        System.setIn(new ByteArrayInputStream("nope\n".getBytes()));
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = pool.submit(() -> repeatDelete(comparator, client, 0, 1));
            Future<?> second = pool.submit(() -> repeatDelete(comparator, client, 1, 1));
            int failures = 0;
            int successes = 0;
            for (Future<?> future : List.of(first, second)) {
                try {
                    future.get(3, TimeUnit.SECONDS);
                    successes++;
                } catch (ExecutionException e) {
                    failures++;
                    Throwable cause = e.getCause();
                    if (cause instanceof InvocationTargetException) {
                        cause = cause.getCause();
                    }
                    assertTrue(cause instanceof IllegalStateException);
                }
            }
            assertEquals(1, failures);
            assertEquals(1, successes);
        } finally {
            pool.shutdownNow();
            System.setIn(original);
        }
        assertTrue(comparator.forceTerminate);
        assertTrue(client.policies.isEmpty());
    }

    private static void repeatDelete(ClusterComparator comparator, AerospikeClientAccess client, int ordinal, int times) {
        for (int i = 0; i < times; i++) {
            try {
                customAction(comparator, client, ordinal);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private static void customAction(ClusterComparator comparator, AerospikeClientAccess client, int ordinal) throws Exception {
        Method method = ClusterComparator.class.getDeclaredMethod(
                "customRecordAction", AerospikeClientAccess.class, Key.class, int.class);
        method.setAccessible(true);
        method.invoke(comparator, client, new Key("test", "users", "k" + ordinal), ordinal);
    }

    private static void runWorker(ClusterComparator comparator, AerospikeClientAccess client) throws Exception {
        Field threads = ClusterComparator.class.getDeclaredField("threadsToUse");
        threads.setAccessible(true);
        threads.setInt(comparator, 1);
        Field active = ClusterComparator.class.getDeclaredField("activeThreads");
        active.setAccessible(true);
        active.set(comparator, new AtomicInteger(1));
        Field list = ClusterComparator.class.getDeclaredField("partitionList");
        list.setAccessible(true);
        List<Integer> partitions = new ArrayList<>();
        partitions.add(7);
        partitions.add(8);
        list.set(comparator, partitions);

        Class<?> runnerClass = null;
        for (Class<?> candidate : ClusterComparator.class.getDeclaredClasses()) {
            if ("PartitionCompareRunner".equals(candidate.getSimpleName())) {
                runnerClass = candidate;
            }
        }
        Constructor<?> constructor = runnerClass.getDeclaredConstructor(
                ClusterComparator.class, AerospikeClientAccess[].class, String.class, String.class);
        constructor.setAccessible(true);
        Runnable runner = (Runnable) constructor.newInstance(
                comparator, new AerospikeClientAccess[] {client, client}, "test", null);
        runner.run();
    }

    @SuppressWarnings("unchecked")
    private static List<Integer> failedPartitions(ClusterComparator comparator) throws Exception {
        Field field = ClusterComparator.class.getDeclaredField("failedPartitionsList");
        field.setAccessible(true);
        return new ArrayList<>((List<Integer>) field.get(comparator));
    }

    @SuppressWarnings("unchecked")
    private static List<Integer> partitionList(ClusterComparator comparator) throws Exception {
        Field field = ClusterComparator.class.getDeclaredField("partitionList");
        field.setAccessible(true);
        return new ArrayList<>((List<Integer>) field.get(comparator));
    }

    private static WritePolicy writePolicy(ClusterComparator comparator) throws Exception {
        Field field = ClusterComparator.class.getDeclaredField("writePolicyToUse");
        field.setAccessible(true);
        return (WritePolicy) field.get(comparator);
    }

    private static String summary(ClusterComparator comparator) throws Exception {
        PrintStream original = System.out;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        System.setOut(new PrintStream(buffer));
        try {
            Method showSummary = ClusterComparator.class.getDeclaredMethod("showSummary");
            showSummary.setAccessible(true);
            showSummary.invoke(comparator);
        } finally {
            System.setOut(original);
        }
        return buffer.toString();
    }

    private static ClusterComparator comparator(String... extra) throws Exception {
        List<String> args = new ArrayList<>();
        args.add("--hosts1");
        args.add("h1:3000");
        args.add("--hosts2");
        args.add("h2:3000");
        args.add("--namespaces");
        args.add("test");
        for (String arg : extra) {
            if (!arg.isEmpty()) {
                args.add(arg);
            }
        }
        return new ClusterComparator(new ClusterComparatorOptions(args.toArray(new String[0])));
    }

    private static final class RecordingClient extends StubAerospikeClient {
        private final List<WritePolicy> policies = new java.util.concurrent.CopyOnWriteArrayList<>();

        @Override
        public void delete(WritePolicy policy, Key key) {
            policies.add(policy);
        }
    }
}

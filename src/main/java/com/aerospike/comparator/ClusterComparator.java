package com.aerospike.comparator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.ClusterUtilities;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.Statement;
import com.aerospike.comparator.ClusterComparatorOptions.Action;
import com.aerospike.comparator.ClusterComparatorOptions.CompareMode;
import com.aerospike.comparator.dbaccess.AerospikeClientAccess;
import com.aerospike.comparator.dbaccess.LocalAerospikeClient;
import com.aerospike.comparator.dbaccess.RecordSetAccess;
import com.aerospike.comparator.dbaccess.RemoteAerospikeClient;
import com.aerospike.comparator.dbaccess.RemoteServer;

public class ClusterComparator {

    // TODO:
    // - config file to remove certain bins from comparison of sets
    // - Protobuf'ing binary fields
    // - Or msgpack
    // - Ability to compare different sets (scan/batch comparator) Note: how to ensure it's not just a one-way comparison?
    
    private final int startPartition;
    private final int endPartition;
    private final AtomicLongArray recordsProcessedOnCluster;
    private final AtomicLongArray recordsMissingOnCluster;
    private final AtomicLong recordsDifferentCount = new AtomicLong();
    private final AtomicLong totalMissingRecords = new AtomicLong();
    private final AtomicLong totalRecordsCompared = new AtomicLong();
    private final AtomicLong recordsRemaining = new AtomicLong();
    private final AtomicBoolean[] partitionsComplete;
    
    private ExecutorService executor = null;
    private AtomicInteger activeThreads;
    private volatile boolean forceTerminate = false;
    private final List<MissingRecordHandler> missingRecordHandlers = new ArrayList<>();
    private final List<RecordDifferenceHandler> recordDifferenceHandlers = new ArrayList<>();
    private final ClusterComparatorOptions options;
    private int threadsToUse;
    private List<Integer> partitionList = new ArrayList<>();
    private Expression filterExpresion = null;
    private final int numberOfClusters;
    
    private class InternalHandler implements MissingRecordHandler, RecordDifferenceHandler {
        private void checkDifferencesCount() {
            if (options.getMissingRecordsLimit() > 0 && (totalMissingRecords.get() + recordsDifferentCount.get() >= options.getMissingRecordsLimit())) {
                forceTerminate = true;
            }
        }
        
        @Override
        public void handle(int partitionId, Key key, List<Integer> missingFromClusters) throws IOException {
            for (int thisCluster : missingFromClusters ) {
                recordsMissingOnCluster.incrementAndGet(thisCluster);
            }
            totalMissingRecords.incrementAndGet();
            checkDifferencesCount();
        }

        @Override
        public void handle(int partitionId, Key key, Record side1, Record side2, DifferenceCollection differences)
                throws IOException {
            recordsDifferentCount.incrementAndGet();
            checkDifferencesCount();
        }
    }
        
    public ClusterComparator(ClusterComparatorOptions options) throws IOException {
        this.options = options;
        this.startPartition = Math.max(options.getStartPartition(), 0);
        this.endPartition = Math.min(4096, options.getEndPartition());
        int partitionCount = endPartition - startPartition;
        this.partitionsComplete = new AtomicBoolean[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            partitionsComplete[i] = new AtomicBoolean(false);
        }
        numberOfClusters = this.options.getClusterConfigs().size();
        recordsProcessedOnCluster = new AtomicLongArray(numberOfClusters);
        recordsMissingOnCluster = new AtomicLongArray(numberOfClusters);

        InternalHandler handler = new InternalHandler();
        this.missingRecordHandlers.add(handler);
        this.recordDifferenceHandlers.add(handler);
        if (options.isConsole()) {
            ConsoleDifferenceHandler consoleHandler = new ConsoleDifferenceHandler();
            this.missingRecordHandlers.add(consoleHandler);
            this.recordDifferenceHandlers.add(consoleHandler);
        }
        if (options.getOutputFileName() != null && options.getAction() != Action.TOUCH && options.getAction() != Action.READ ) {
            CsvDifferenceHandler csvHandler = new CsvDifferenceHandler(options.getOutputFileName(), options);
            this.missingRecordHandlers.add(csvHandler);
            this.recordDifferenceHandlers.add(csvHandler);
        }
    }
    
    private interface ClusterIterator {
        void perform(int index, ClusterConfig config);
    }
    private void forEachCluster(ClusterIterator iterator) {
        for (int i = 0; i < numberOfClusters; i++) {
            iterator.perform(i, options.getClusterConfigs().get(i));;
        }
    }

    private void addInPartitions(StringBuilder sb, int runCount, int current) {
        if (sb.length() > 1 && runCount > 0) {
            sb.append(',');
        }
        if (runCount == 1) {
            sb.append(this.startPartition + current-1);
        }
        else if (runCount > 1) {
            sb.append(this.startPartition + current-runCount).append('-').append(this.startPartition + current-1);
        }
    }
    
    private String getPartitionsComplete() {
        StringBuilder sb = new StringBuilder().append('[');
        int partitionCount = endPartition - startPartition;
        int runCount = 0;
        for (int i = 0; i < partitionCount; i++) {
            if (partitionsComplete[i].get()) {
                if (runCount > 0) {
                    runCount++;
                }
                else {
                    runCount = 1;
                }
            }
            else {
                if (runCount > 0) {
                    addInPartitions(sb, runCount, i);
                    runCount = 0;
                }
            }
        }
        addInPartitions(sb, runCount, partitionCount);
        return sb.append(']').toString();
    }
    private String tlsPolicyAsString(TlsPolicy policy) {
        if (policy == null) {
            return "null";
        }
        else {
            return String.format("protocols: %s, ciphers: %s, revokeCertificates: %s, forLoginOnly: %b",
                    policy.protocols == null ? "null" : "[" + String.join(",", policy.protocols) + "]",
                    policy.ciphers == null ? "null" : "[" + String.join(",", policy.ciphers) + "]",
                    policy.revokeCertificates == null ? "null" : "[" + policy.revokeCertificates.length + " items]",
                    policy.forLoginOnly
                );
        }
    }
    
    public AerospikeClientAccess connectClient(int clusterIndex, ClusterConfig config) {
        ClientPolicy clientPolicy = new ClientPolicy();
        
        clientPolicy.user = config.getUserName();
        clientPolicy.password = config.getPassword();
        clientPolicy.tlsPolicy = config.getTls() == null ? null : config.getTls().toTlsPolicy();
        clientPolicy.authMode = config.getAuthMode();
        clientPolicy.clusterName = config.getClusterName();
        clientPolicy.useServicesAlternate = config.isUseServicesAlternate();
        String hostNames = config.getHostName();
        if (hostNames.startsWith("remote:")) {
            // Ignore any addresses after the first
            int index = hostNames.indexOf(',');
            if (index > 0) {
                hostNames = hostNames.substring(0, index);
            }
            String[] remoteHost = hostNames.split(":");
            if (remoteHost.length != 3) {
                System.out.printf("If using a remote server, the address must be specified in the format: 'remote:<host_ip>:<port>', but received '%s'\n", hostNames);
                System.exit(-1);
            }
            try {
                if (!options.isSilent()) {
                    System.out.printf("Remote cluster %d: hosts: %s tlsPolicy: %s\n", 
                            clusterIndex, hostNames, tlsPolicyAsString(clientPolicy.tlsPolicy));
                }
                return new RemoteAerospikeClient(remoteHost[1], Integer.valueOf(remoteHost[2]), this.threadsToUse, clientPolicy.tlsPolicy, options);
            }
            catch (IOException ioe) {
                throw new AerospikeException(ioe);
            }
        }
        else {
            Host[] hosts = Host.parseHosts(hostNames, 3000);
            
            if (clientPolicy.user != null && clientPolicy.password == null) {
                java.io.Console console = System.console();
                if (console != null) {
                    char[] pass = console.readPassword("Enter password for cluster " + clusterIndex + ": ");
                    if (pass != null) {
                        clientPolicy.password = new String(pass);
                    }
                }
            }
            IAerospikeClient client = new AerospikeClient(clientPolicy, hosts);
            if (!options.isSilent()) {
                System.out.printf("Cluster %d: name: %s, hosts: %s user: %s, password: %s\n", clusterIndex, 
                        clientPolicy.clusterName, Arrays.toString(hosts), clientPolicy.user, clientPolicy.password == null ? "null" : "********");
                System.out.printf("         authMode: %s, tlsPolicy: %s\n", clientPolicy.authMode, tlsPolicyAsString(clientPolicy.tlsPolicy));
                if (options.isVerbose()) {
                    new ClusterUtilities(client).printInfo(true, 120);
                }
            }
            return new LocalAerospikeClient(client);
        }
    }

    private List<Integer> quickCompare(AerospikeClientAccess[] clients, String namespace) {
        PartitionMap[] partitionMaps = new PartitionMap[clients.length];
        Set<Integer> partitionsDifferent = new HashSet<>();
        forEachCluster((i, c) -> {
            PartitionMap partitionMap = new PartitionMap(clients[i]);
            if (!partitionMap.isComplete(namespace)) {
                throw new QuickCompareException("Not all partitions are available for namespace '" + namespace + "' on cluster 1, quick compare is not available.");
            }
            if (partitionMap.isMigrationsHappening(namespace)) {
                throw new QuickCompareException("Migrations are happening for namespace '" + namespace + "' on cluster 1, quick compare is not available.");
            }
            partitionMaps[i] = partitionMap;
        });
        if (!options.isSilent()) {
            System.out.printf("Quick record counts:\n");
            forEachCluster((i, c) -> {
                System.out.printf("\tcluster %d: (%d records, %d tombstones)\n",
                    i, partitionMaps[i].getRecordCount(namespace), partitionMaps[i].getTombstoneCount(namespace));
            });
        }
        for (int i = 0; i < clients.length; i++) {
            for (int j = i+1; j < clients.length; j++) {
                partitionsDifferent.addAll(partitionMaps[i].compare(partitionMaps[j], namespace));
            }
        }
        List<Integer> differences = new ArrayList<>(partitionsDifferent);
        differences.sort(null);
        return differences;
    }
    
    private int compare(byte[] digest1, byte[] digest2) {
        if (digest1.length != digest2.length) {
            throw new IllegalArgumentException("Digest1 has a length of " + digest1.length + ", digest2 has a length of " + digest2.length);
        }
        for (int i = 0; i < digest1.length; i++) {
            char c1 = (char) digest1[i];
            char c2 = (char) digest2[i];
            if (c1 < c2) {
                return -1;
            }
            if (c1 > c2) {
                return 1;
            }
        }
        return 0;
    }

    private void differentRecords(int partitionId, Key key, DifferenceCollection differences) {
        for (RecordDifferenceHandler thisHandler : recordDifferenceHandlers) {
            try {
                thisHandler.handle(partitionId, key, null, null, differences);
            }
            catch (Exception e) {
                System.err.printf("Error in %s: %s\n", thisHandler.getClass().getSimpleName(), e.getMessage());
                e.printStackTrace();
                this.forceTerminate = true;
            }
        }
    }

//    private void differentRecords(int partitionId, Key key, Record record1, Record record2, DifferenceSet differences) {
//        for (RecordDifferenceHandler thisHandler : recordDifferenceHandlers) {
//            try {
//                thisHandler.handle(partitionId, key, record1, record2, differences);
//            }
//            catch (Exception e) {
//                System.err.printf("Error in %s: %s\n", thisHandler.getClass().getSimpleName(), e.getMessage());
//                e.printStackTrace();
//                this.forceTerminate = true;
//            }
//        }
//    }
    private void missingRecord(AerospikeClientAccess[] clients, List<Integer> clustersWithRecord, List<Integer> clustersWithoutRecord, int partitionId, Key keyMissing) {
        for (MissingRecordHandler thisHandler : missingRecordHandlers) {
            try {
                thisHandler.handle(partitionId, keyMissing, clustersWithoutRecord);
            }
            catch (Exception e) {
                System.err.printf("Error in %s: %s\n", thisHandler.getClass().getSimpleName(), e.getMessage());
                e.printStackTrace();
                this.forceTerminate = true;
            }
        }
        // TODO: Should we touch / read all the clusters with the record or just one? 
        if (this.options.getAction() == Action.SCAN_TOUCH) {
            for (Integer thisCluster : clustersWithRecord) {
                touchRecord(clients[thisCluster], keyMissing);
            }
        }
        if (this.options.getAction() == Action.SCAN_READ) {
            for (Integer thisCluster : clustersWithRecord) {
                readRecord(clients[thisCluster], keyMissing);
            }
        }
    }
    
    private boolean getNextRecord(RecordSetAccess recordSet, int cluster) {
        boolean result = recordSet.next();
        if (result) {
            recordsProcessedOnCluster.incrementAndGet(cluster);
        }
        return result;
    }
    
    private Expression formFilterExpression() {
        if (this.options.getBeginDate() == null && this.options.getEndDate() == null) {
            return null;
        }
        if (this.options.getBeginDate() == null && this.options.getEndDate() != null) {
            // We need the last update time to be <= endTime
            return Exp.build(Exp.le(Exp.lastUpdate(), Exp.val(TimeUnit.MILLISECONDS.toNanos(this.options.getEndDate().getTime()))));
        }
        else if (this.options.getBeginDate() != null && this.options.getEndDate() == null) {
            // We need the last update time to be greater or equal to the begin time.
            return Exp.build(Exp.ge(Exp.lastUpdate(), Exp.val(TimeUnit.MILLISECONDS.toNanos(this.options.getBeginDate().getTime()))));
        }
        else {
            // Both are non-null
            return Exp.build(Exp.and(
                    Exp.le(Exp.lastUpdate(), Exp.val(TimeUnit.MILLISECONDS.toNanos(this.options.getEndDate().getTime()))), 
                    Exp.ge(Exp.lastUpdate(), Exp.val(TimeUnit.MILLISECONDS.toNanos(this.options.getBeginDate().getTime())))));
        }
    }
    
    private boolean anySideValid(boolean[] sidesValid) {
        for (boolean isValid: sidesValid) {
            if (isValid) {
                return true;
            }
        }
        return false;
    }
    
    private Key getKeyWithLargestDigest(Key[] keys) {
        if (keys.length == 1) {
            throw new IllegalArgumentException("Must pass at least two keys");
        }
        Key keyToReturn = keys[0];
        for (int i = 1; i < keys.length; i++) {
            if (keyToReturn == null) {
                keyToReturn = keys[i];
            }
            else {
                if (keys[i] != null) {
                    int result = compare(keyToReturn.digest, keys[i].digest);
                    if (result < 0) {
                        // max < keys[i].digest
                        keyToReturn = keys[i];
                    }
                }
            }
        }
        return keyToReturn;
    }
    
    private void comparePartition(AerospikeClientAccess[] clients, String namespace, String setName, int partitionId) {
        QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.maxConcurrentNodes = 1;
        queryPolicy.includeBinData = options.isRecordLevelCompare();
        queryPolicy.shortQuery = false;
        queryPolicy.filterExp = this.filterExpresion;
        
        Statement statement = new Statement();
        statement.setNamespace(namespace);
        statement.setSetName(setName);

        int rpsThisThread = options.getRps()/this.threadsToUse;
        if (options.getRps() > 0 && rpsThisThread == 0) {
            // Eg 10 threads, 5 rps would give 0
            rpsThisThread = 1;
        }
        statement.setRecordsPerSecond(rpsThisThread);
        
        PartitionFilter[] filters = new PartitionFilter[clients.length];
        for (int i = 0; i < clients.length; i++) {
            filters[i] = PartitionFilter.id(partitionId);
        }
        
        if (options.isDebug()) {
            System.out.printf("Thread %d starting comparison of namespace %s, partition %d\n", Thread.currentThread().getId(), namespace, partitionId);
        }
        RecordSetAccess[] recordSets = new RecordSetAccess[clients.length];
        boolean[] sidesValid = new boolean[clients.length];
        for (int i = 0; i < clients.length; i++) {
            recordSets[i] = clients[i].queryPartitions(queryPolicy, statement, filters[i]);
            sidesValid[i] = getNextRecord(recordSets[i], i);
        }
        
        RecordComparator comparator = new RecordComparator();

        try {
            Key[] keys = new Key[clients.length];
            List<Integer> clustersWithMaxDigest = new ArrayList<>();
            List<Integer> clustersWithoutRecord = new ArrayList<>();
            while (anySideValid(sidesValid) && !forceTerminate) {
                for (int i = 0; i < clients.length; i++) {
                    keys[i] = sidesValid[i] ? recordSets[i].getKey() : null;
                }
                // The digests go down as we go through the partition, so we need to find the largest
                // digest, and any clusters which don't have that digest are missing records and get
                // flagged. Any clusters with the max need to advance to the next record.
                Key keyWithLargestDigest = getKeyWithLargestDigest(keys);
                clustersWithMaxDigest.clear();
                clustersWithoutRecord.clear();
                if (options.isDebug()) {
                    System.out.printf("Next digest: %s, cluster digests:", keyWithLargestDigest);
                    forEachCluster((i, c)-> System.out.printf(" %d:%s", i, keys[i]));
                    System.out.println();
                }
                for (int i = 0; i < clients.length; i++) {
                    if (keys[i] == null) {
                        clustersWithoutRecord.add(i);
                    }
                    else if (compare(keys[i].digest, keyWithLargestDigest.digest) == 0) {
                        // This is valid, advance or compare
                        clustersWithMaxDigest.add(i);
                    }
                    else {
                        clustersWithoutRecord.add(i);
                    }
                }
                if (!clustersWithoutRecord.isEmpty()) 
                    missingRecord(clients, clustersWithMaxDigest, clustersWithoutRecord, partitionId, keyWithLargestDigest);
                
                // Need to compare the records with the maximum digest
                if (options.isRecordLevelCompare()) {
                    compareRecords(comparator, partitionId, clustersWithMaxDigest, clients, recordSets, keys);
                }
                for (int cluster : clustersWithMaxDigest) {
                    sidesValid[cluster] = getNextRecord(recordSets[cluster], cluster);
                }
            }
        }
        finally {
            for (RecordSetAccess recordSet : recordSets) {
                try {
                    recordSet.close();
                }
                catch (Exception ignored) {}
            }
            partitionsComplete[partitionId-startPartition].set(true);
        }
    }

    private void compareRecord(DifferenceCollection differenceCollection, RecordComparator comparator, int partitionId, AerospikeClientAccess client1, AerospikeClientAccess client2, RecordSetAccess recordSet1, RecordSetAccess recordSet2, Key key1, Key key2, int cluster1index, int cluster2index) {
        DifferenceSet compareResult = null;
        if ((client1.isLocal() && client2.isLocal()) || !options.isRemoteServerHashes()) {
            Record record1 = recordSet1.getRecord();
            Record record2 = recordSet2.getRecord();
            compareResult = comparator.compare(key1, record1, record2,
                    options.getPathOptions(),
                    options.getCompareMode() == CompareMode.RECORDS_DIFFERENT, cluster1index, cluster2index);
        }
        else {
            byte[] record1hash = recordSet1.getRecordHash(options.isSortMaps());
            byte[] record2hash = recordSet2.getRecordHash(options.isSortMaps());
            int recordsEqual = compare(record1hash, record2hash);
            if (recordsEqual != 0) {
                if (options.getCompareMode() == CompareMode.RECORDS_DIFFERENT) {
                    compareResult = comparator.compare(key1, record1hash, record2hash,
                            options.getPathOptions(), cluster1index, cluster2index);
                }
                else {
                    Record record1 = client1.isLocal() ? recordSet1.getRecord() : client1.get(null, key1);
                    Record record2 = client2.isLocal() ? recordSet2.getRecord() : client2.get(null, key2);
                    compareResult = comparator.compare(key1, record1, record2,
                            options.getPathOptions(), false, cluster1index, cluster2index);
                }
            }
        }
        differenceCollection.add(compareResult);
    }
    
    private DifferenceCollection compareRecords(RecordComparator comparator, int partitionId, List<Integer> clustersToCompare, AerospikeClientAccess[] clients, RecordSetAccess[] recordSets, Key[] keys) {
        DifferenceCollection compareResult = new DifferenceCollection(clustersToCompare);
        for (int i = 0; i < clustersToCompare.size(); i++) {
            for (int j = i+1; j < clustersToCompare.size(); j++) {
                int left = clustersToCompare.get(i);
                int right = clustersToCompare.get(j);
                compareRecord(compareResult, comparator, partitionId,
                        clients[left], clients[right],
                        recordSets[left], recordSets[right],
                        keys[left], keys[right],
                        left, right
                );
            }
        }
        
        if (compareResult.hasDifferences()) {
            differentRecords(partitionId, keys[0], compareResult);
        }
        long totalRecsCompared = totalRecordsCompared.incrementAndGet();
        if (options.getRecordCompareLimit() > 0 && totalRecsCompared >= options.getRecordCompareLimit()) {
            forceTerminate = true;
        }
        return compareResult;
    }
    
    /**
     * The PartitionCompareRunner is used to compare a set of partitions. The partitions used are specified in the partitionList variable 
     * @author tfaulkes
     */
    private class PartitionCompareRunner implements Runnable {
        private final AerospikeClientAccess[] clients;
        private final String namespace;
        private final String setName;
        
        public PartitionCompareRunner(AerospikeClientAccess[] clients, String namespace, String setName) {
            super();
            this.clients = clients;
            this.namespace = namespace;
            this.setName = setName;
        }

        public void run() {
            boolean done = false;
            try {
                while (!done && !forceTerminate) {
                    int partitionId;
                    synchronized (partitionList) {
                        if (partitionList.isEmpty()) {
                            done = true;
                            partitionId = -1;
                        }
                        else {
                            partitionId = partitionList.remove(0);
                        }
                    }
                    if (!done) {
                        comparePartition(clients, namespace, setName, partitionId);
                    }
                }
            }
            finally {
                activeThreads.decrementAndGet();
            }
        }
    }
    
    /**
     * The FixedRecordsCompareRunner is used to compare a set of records which are not necessarily in the same partition. This is 
     * used when loading the records from an input file such as the output of a previous run for example. 
     * @author tfaulkes
     */
    private class FixedRecordsCompareRunner implements Runnable {
        private final AerospikeClientAccess[] clients;
        private final FileLoadingProcessor processor;
        
        public FixedRecordsCompareRunner(AerospikeClientAccess[] clients, FileLoadingProcessor processor) {
            super();
            this.clients = clients;
            this.processor = processor;
        }

        public void run() {
            try {
                RecordComparator comparator = new RecordComparator();

                while (!processor.isDone() && !forceTerminate) {
                    
                    FileLine line = processor.get();
                    if (line == null) {
                        break;
                    }
                    Key key = line.getKey();
                    int partId = Partition.getPartitionId(key.digest);
                    if (options.getCompareMode() == CompareMode.MISSING_RECORDS) {
                        List<Integer> clientsWithRecord = new ArrayList<>();
                        List<Integer> clientsWithoutRecord = new ArrayList<>();
                        forEachCluster((i, c) -> {
                            if (clients[i].exists(null, key)) {
                                clientsWithRecord.add(i);
                            }
                            else {
                                clientsWithoutRecord.add(i);
                            }
                        });
                        if (clientsWithoutRecord.size() > 0) {
                            missingRecord(clients, clientsWithRecord, clientsWithoutRecord, partId, key);
                        }
                    }
                    else {
                        // This must be a record level compare
                        DifferenceSet compareResult = null;
                        Record[] records = new Record[clients.length];
                        List<Integer> clustersWithRecord = new ArrayList<>();
                        List<Integer> clustersWithoutRecord = new ArrayList<>();
                        for (int i = 0; i < clients.length; i++) {
                            records[i] = clients[i].get(null, key);
                            if (records[i] == null) {
                                clustersWithoutRecord.add(i);
                            }
                            else {
                                clustersWithRecord.add(i);
                            }
                        }
                        missingRecord(clients, clustersWithRecord, clustersWithoutRecord, partId, key);
                        // For those with the record, iterate through all clusters with the record and compare them.
                        DifferenceCollection differenceCollection = new DifferenceCollection(clustersWithRecord);
                        for (int i = 0; i < clustersWithRecord.size(); i++) {
                            for (int j = i+1; j < clustersWithRecord.size(); j++) {
                                int left = clustersWithRecord.get(i);
                                int right = clustersWithRecord.get(j);
                                compareResult = comparator.compare(key, records[left], records[right],
                                        options.getPathOptions(),
                                        options.getCompareMode() == CompareMode.RECORDS_DIFFERENT, left, right);
                                differenceCollection.add(compareResult);
                            }
                        }
                        if (differenceCollection.hasDifferences()) {
                            differentRecords(partId, key, differenceCollection);
                        }
                        long totalRecsCompared = totalRecordsCompared.incrementAndGet();
                        if (options.getRecordCompareLimit() > 0 && totalRecsCompared >= options.getRecordCompareLimit()) {
                            forceTerminate = true;
                        }
                        
                    }
                }
            }
            catch (InterruptedException ie) {
                // ignored
            }
            finally {
                activeThreads.decrementAndGet();
            }
        }
    }
    
    private void beginComparison(AerospikeClientAccess[] clients, String namespace, String setName) throws InterruptedException {
        Runnable runner = null;
        if (options.isQuickCompare()) {
            try {
                List<Integer> partitionsToCompare = quickCompare(clients, namespace);
                if (!options.isSilent()) {
                    if (partitionsToCompare.size() <= 20) {
                        System.out.printf("Quick compare found %d partitions different: %s\n", partitionsToCompare.size(), partitionsToCompare);
                    }
                    else {
                        System.out.printf("Quick compare found %d partitions different\n", partitionsToCompare.size());
                    }
                }
                return;
            }
            catch (AerospikeException ae) {
                if (ae.getResultCode() == ResultCode.ROLE_VIOLATION) {
                    System.out.println("Quick compare cannot be run, passed user does not have authorization to run Info functions");
                    return;
                }
                else {
                    throw ae;
                }
            }
            catch (QuickCompareException qce) {
                System.out.println(qce.getMessage());
                return;
            }
        }
        else if (options.getAction() == Action.RERUN) {
            runner = new FixedRecordsCompareRunner(clients, new FileLoadingProcessor());
        }
        else {
            this.partitionList = IntStream.range(startPartition, endPartition).boxed().collect(Collectors.toList());
            runner = new PartitionCompareRunner(clients, namespace, setName);
        }
        
        this.executor = Executors.newFixedThreadPool(threadsToUse);
        this.activeThreads = new AtomicInteger(threadsToUse);
        for (int i = 0; i < threadsToUse; i++) {
            this.executor.execute(runner);
        }
        this.executor.shutdown();
        this.monitorProgress(namespace, setName);
    }
    
    private void performComparisons(AerospikeClientAccess[] clients) throws InterruptedException {
        this.filterExpresion = formFilterExpression();
        for (int i = 0; i < clients.length; i++) {
            recordsMissingOnCluster.set(i, 0);
            recordsProcessedOnCluster.set(i, 0);
        }

        if (options.isMetadataCompare()) {
            MetadataComparator metadataComparator = new MetadataComparator(options);
            DifferenceSet result = metadataComparator.compareMetaData(clients);
            // TODO: Should the result be used in the summary?
        }
        if (options.getAction() == Action.RERUN) {
            beginComparison(clients, null, null);
        }
        else {
            for (String namespace : options.getNamespaces()) {
                String[] sets = options.getSetNames();
                if (sets == null || sets.length == 0) {
                    beginComparison(clients, namespace, null);
                }
                else {
                    for (String thisSet : sets) {
                        beginComparison(clients, namespace, thisSet);
                    }
                }
            }
        }
        for (MissingRecordHandler thisHandler : missingRecordHandlers) {
            thisHandler.close();
        }
    }
    
    private void monitorProgress(String namespace, String setName) throws InterruptedException {
        if (!options.isSilent()) {
            if (options.getAction().needsInputFile() ) {
                System.out.println("Comparison started using input file: " + options.getInputFileName());
            }
            else {
                System.out.println("Comparison started for namespace " + namespace + ((setName == null) ?  "." : (", set " + setName + ".")));
            }
        }
        long[] lastRecordsForCluster = new long[numberOfClusters];
        long[] currentRecordsForCluster = new long[numberOfClusters];
        forEachCluster((i, c) -> lastRecordsForCluster[i] = 0);
        
        long startTime = System.currentTimeMillis();
        while (activeThreads.get() > 0) {
            Thread.sleep(1000);
            long totalCurrentRecords = 0;
            long recordsThisSecond = 0;
            for (int i = 0; i < numberOfClusters; i++) {
                currentRecordsForCluster[i] = this.recordsProcessedOnCluster.get(i);
                totalCurrentRecords += currentRecordsForCluster[i];
                recordsThisSecond += currentRecordsForCluster[i] - lastRecordsForCluster[i];
            };
            int nextPartition = this.partitionList.size();
            int activeThreads = this.activeThreads.get();
            long now = System.currentTimeMillis();
            long elapsedMilliseconds = now - startTime;
            if (!options.isSilent()) {
                if (options.getAction() != Action.RERUN) {
                    System.out.printf("%,dms: [%d-%d, remaining %d, complete:%s], active threads: %d, records processed: {",
                            (now-startTime), this.startPartition, this.endPartition, nextPartition, getPartitionsComplete(), activeThreads);
                }
                else {
                    long remaining = recordsRemaining.get();
                    System.out.printf("%,dms: [buffer lines: %d%s], active threads: %d, records processed: {", 
                            (now-startTime), remaining, remaining == FileLoadingProcessor.MAX_QUEUE_DEPTH ? "+" : "", activeThreads);
                }
                forEachCluster((i, c) -> System.out.printf("%scluster%d: %,d", i > 0 ? ", ": "" ,i, currentRecordsForCluster[i]));
                System.out.printf("} throughput: {last second: %,d rps, overall: %,d rps}\n", 
                        recordsThisSecond/numberOfClusters,
                        (totalCurrentRecords)*1000/2/elapsedMilliseconds);
            }
            forEachCluster((i, c) -> lastRecordsForCluster[i] = currentRecordsForCluster[i]);
        }
        this.executor.awaitTermination(7, TimeUnit.DAYS);
        
        if (!options.isSilent()) {
            if (options.isRecordLevelCompare()) {
                forEachCluster((i, c) -> System.out.printf("Missing records on side %d : %,d\n", i, this.recordsMissingOnCluster.get(i)));
                System.out.printf("Records different         : %,d\n"
                                + "Records compared          : %,d\n", 
                        this.recordsDifferentCount.get(), this.totalRecordsCompared.get());
            }
            else {
                forEachCluster((i, c) -> System.out.printf("Missing records on side %d : %,d\n", i, this.recordsMissingOnCluster.get(i)));
            }
            if (this.forceTerminate) {
                if (this.totalMissingRecords.get() >= this.options.getMissingRecordsLimit()) {
                    System.out.printf("Comparison terminated after finding %d missing records on a limit of %d\n", 
                            this.totalMissingRecords.get(), this.options.getMissingRecordsLimit());
                }
                else if (this.totalRecordsCompared.get() >= this.options.getRecordCompareLimit()) {
                    System.out.printf("Comparison terminated after comparing %d records on a limit of %d\n", 
                            this.totalRecordsCompared.get(), this.options.getRecordCompareLimit());
                }
            }
        }
    }
    
    private void touchRecord(AerospikeClientAccess client, Key key) {
        try {
            client.touch(null, key);
            if (!options.isSilent()) {
                System.out.println("Touching record " + key);
            }
        }
        catch (AerospikeException ae) {
            System.out.printf("Error thrown when touching record. Error was %s, class %s\n", ae.getMessage(), ae.getClass().getCanonicalName());
            System.out.printf("Key: %s\n", key);
            ae.printStackTrace();
        }
    }

    private void readRecord(AerospikeClientAccess client, Key key) {
        try {
            client.get(null, key);
            if (!options.isSilent()) {
                System.out.println("Reading record " + key);
            }
        }
        catch (AerospikeException ae) {
            System.out.printf("Error thrown when reading record. Error was %s, class %s\n", ae.getMessage(), ae.getClass().getCanonicalName());
            System.out.printf("Key: %s\n", key);
            ae.printStackTrace();
        }
    }

    private final FileLineProcessor TOUCH_RECORD_PROCESSOR = new FileLineProcessor() {
        @Override
        public void process(AerospikeClientAccess[] clients, FileLine line) {
            forEachCluster((i, c) -> {
                if (line.hasDigest(i)) {
                    touchRecord(clients[i], line.getKey(i));
                    return;
                }
            });
        }
    };
    
    private final FileLineProcessor READ_RECORD_PROCESSOR = new FileLineProcessor() {
        @Override
        public void process(AerospikeClientAccess[] clients, FileLine line) {
            forEachCluster((i, c) -> {
                if (line.hasDigest(i)) {
                    readRecord(clients[i], line.getKey(i));
                    return;
                }
            });
        }
    };
    
    /**
     * This processor simply loads lines from a file and serves them through a queue. The queue is thread
     * safe and is designed so multiple threads will pull lines to be processed and the internal class will
     * keep reading the file and topping up the line buffer.
     * @author tfaulkes
     *
     */
    private class FileLoadingProcessor implements FileLineProcessor {
        public static final int MAX_QUEUE_DEPTH = 10000;
        private ArrayBlockingQueue<FileLine> lines = new ArrayBlockingQueue<>(MAX_QUEUE_DEPTH);
        private volatile boolean done = false;
        
        public FileLoadingProcessor() {
            Thread producer = new Thread(() -> {
                try {
                    processRecords(null, (clients, line) -> {
                        try {
                            lines.put(line);
                        } catch (InterruptedException e) {
                            System.err.println("InterruptedException reading file: " + e.getMessage());
                        }
                    }, options.getInputFileName());
                } catch (IOException e) {
                    System.err.println("IOException reading file: " + e.getMessage());
                }
                done = true;
            }, "RecordProducer");
            producer.setDaemon(true);
            producer.start();
        }
        
        public synchronized FileLine get() throws InterruptedException {
            recordsRemaining.set(lines.size());
            if (isDone()) {
                return null;
            }
            return lines.take();
        }
        
        public boolean isDone() {
            return done && lines.isEmpty();
        }
        @Override
        public void process(AerospikeClientAccess[] clients, FileLine line) {
            forEachCluster((i, c) -> {
                if (line.hasDigest(i)) {
                    readRecord(clients[i], line.getKey(i));
                }
            });
        }
    }
    
    private void processRecords(AerospikeClientAccess[] clients, FileLineProcessor processor, String fileName) throws IOException {
        File file = new File(fileName);
        BufferedReader br = new BufferedReader(new FileReader(file));
        
        try {
            String line = br.readLine();
            CsvDifferenceHandler handler = new CsvDifferenceHandler(null, options);
            if (!handler.getFileHeader().equals(line)) {
                throw new UnsupportedOperationException("File " + options.getOutputFileName() + " has a header which does not match what was expected. Expected '" + handler.getFileHeader() + "' but received '" + line + "'");
            }

            while ((line = br.readLine()) != null) {
                processor.process(clients, new FileLine(line));
            }
        } finally {
            br.close();
        }
    }
    
    private void startRemoteServer() {
        AerospikeClientAccess client1 = this.connectClient(0, options.getClusterConfigs().get(0));
        RemoteServer remoteServer = new RemoteServer(client1, options.getRemoteServerPort(), options.getRemoteServerHeartbeatPort(), options.isVerbose(), options.isDebug());
        try {
            remoteServer.start(options.getRemoteServerTls());
        } catch (IOException e) {
            System.err.printf("IOException occurred in remote server mode, terminating server. %s\n", e.getMessage());
            e.printStackTrace();
        }
    }
    
    public DifferenceSummary begin() throws Exception {
        if (!options.isSilent()) {
            System.out.printf("=== Aerospike Cluster Comparator v%s ===\n", this.getClass().getPackage().getImplementationVersion());
        }
        if (options.isRemoteServer()) {
            startRemoteServer();
            return null;
        }
        if (!options.isSilent()) {
            System.out.printf("Beginning scan with namespaces '%s', sets '%s', start partition: %d, end partition %d\n", 
                    String.join(",", this.options.getNamespaces()),
                    this.options.getSetNames() == null ? "null" : String.join(",", this.options.getSetNames()),
                    this.options.getStartPartition(), this.options.getEndPartition());
            if (options.getBeginDate() != null && options.getEndDate() != null) {
                System.out.printf("  Looking only for records between %s (%,d) and %s (%,d)\n", 
                        options.getDateFormat().format(options.getBeginDate()), options.getBeginDate().getTime(),
                        options.getDateFormat().format(options.getEndDate()), options.getEndDate().getTime());
            }
            else if (options.getBeginDate() != null) {
                System.out.printf("  Looking only for records after %s (%,d)\n", 
                        options.getDateFormat().format(options.getBeginDate()), options.getBeginDate().getTime());
            }
            else if (options.getEndDate() != null) {
                System.out.printf("  Looking only for records before %s (%,d)\n", 
                        options.getDateFormat().format(options.getEndDate()), options.getEndDate().getTime());
            }
            Date now = new Date();
            System.out.printf("Run starting at %s (%d) with comparison mode %s\n", options.getDateFormat().format(now), now.getTime(), options.getCompareMode());
        }
        this.threadsToUse = options.getThreads() <= 0 ? Runtime.getRuntime().availableProcessors() : options.getThreads();
        AerospikeClientAccess[] clients = new AerospikeClientAccess[numberOfClusters];
        forEachCluster((i, c) -> clients[i] = this.connectClient(i, c));
        Scanner input = null;
        try {
            if (options.getAction() == Action.TOUCH) {
                this.processRecords(clients, TOUCH_RECORD_PROCESSOR, options.getInputFileName());
            }
            else if (options.getAction() == Action.READ) {
                this.processRecords(clients, READ_RECORD_PROCESSOR, options.getInputFileName());
            }
            else {
                this.performComparisons(clients);
                if (options.getAction() == Action.SCAN_ASK && (totalMissingRecords.get() > 0 || recordsDifferentCount.get() > 0)) {
                    System.out.printf("%,d differences found between the 2 clusters. Do you want those records to be touched, read or none? (t/r//N)", totalMissingRecords.get());
                    input = new Scanner(System.in);
                    FileLineProcessor processor = null;
                    while (true) {
                        String  next = input.nextLine();
                        if (next.trim().toUpperCase().startsWith("T")) {
                            processor = TOUCH_RECORD_PROCESSOR;
                            break;
                        }
                        if (next.trim().toUpperCase().startsWith("R")) {
                            processor = READ_RECORD_PROCESSOR;
                            break;
                        }
                        if (next.trim().toUpperCase().startsWith("N") || next.trim().length() == 0) {
                            break;
                        }
                        System.out.printf("Please enter 't', 'r' or 'n'\n");
                    }
                    if (processor != null) {
                        processRecords(clients, processor, options.getOutputFileName());
                    }
                }
            }
        }
        finally {
            forEachCluster((i,c) -> clients[i].close());
            if (input != null) {
                input.close();
            }
        }
        return new DifferenceSummary(recordsMissingOnCluster, recordsDifferentCount.get());
    }
    
    public static void main(String[] args) throws Exception {
        ClusterComparatorOptions options = new ClusterComparatorOptions(args);
        
        ClusterComparator comparator = new ClusterComparator(options);
        comparator.begin();
        
    }
}

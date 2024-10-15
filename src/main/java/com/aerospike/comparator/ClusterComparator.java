package com.aerospike.comparator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
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
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.Statement;
import com.aerospike.comparator.ClusterComparatorOptions.Action;
import com.aerospike.comparator.ClusterComparatorOptions.CompareMode;
import com.aerospike.comparator.dbaccess.AerospikeClientAccess;
import com.aerospike.comparator.dbaccess.LocalAerospikeClient;
import com.aerospike.comparator.dbaccess.RecordMetadata;
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
    private List<Integer> failedPartitionsList = new ArrayList<>();
    private Expression filterExpresion = null;
    private final int numberOfClusters;
    
    private final Policy tolerantReadPolicy = new Policy();
    private final WritePolicy tolerantWritePolicy = new WritePolicy();
    
    private class InternalHandler implements MissingRecordHandler, RecordDifferenceHandler {
        private void checkDifferencesCount() {
            if (options.getMissingRecordsLimit() > 0 && (totalMissingRecords.get() + recordsDifferentCount.get() >= options.getMissingRecordsLimit())) {
                forceTerminate = true;
            }
        }
        
        @Override
        public void handle(int partitionId, Key key, List<Integer> missingFromClusters, boolean hasRecordLevelDifferences, RecordMetadata[] recordMetadatas) throws IOException {
            for (int thisCluster : missingFromClusters ) {
                recordsMissingOnCluster.incrementAndGet(thisCluster);
            }
            totalMissingRecords.incrementAndGet();
            checkDifferencesCount();
        }

        @Override
        public void handle(int partitionId, Key key, DifferenceCollection differences, List<Integer> missingFromClusters, RecordMetadata[] recordMetadatas) throws IOException {
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

        this.tolerantReadPolicy.totalTimeout = 5000;
        this.tolerantReadPolicy.socketTimeout = 200;
        this.tolerantReadPolicy.maxRetries = 25;
        this.tolerantReadPolicy.sleepBetweenRetries = 500;
        
        this.tolerantWritePolicy.totalTimeout = 5000;
        this.tolerantWritePolicy.socketTimeout = 200;
        this.tolerantWritePolicy.maxRetries = 25;
        this.tolerantWritePolicy.sleepBetweenRetries = 500;
        
        InternalHandler handler = new InternalHandler();
        this.missingRecordHandlers.add(handler);
        this.recordDifferenceHandlers.add(handler);
        if (options.isConsole()) {
            ConsoleDifferenceHandler consoleHandler = new ConsoleDifferenceHandler(this.options);
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
        clientPolicy.minConnsPerNode = this.threadsToUse;

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
            String thisNamespace = options.getNamespaceName(namespace, i);
            PartitionMap partitionMap = new PartitionMap(clients[i]);
            if (!partitionMap.isComplete(thisNamespace)) {
                throw new QuickCompareException("Not all partitions are available for namespace '" + thisNamespace + "' on cluster 1, quick compare is not available.");
            }
            if (partitionMap.isMigrationsHappening(thisNamespace)) {
                throw new QuickCompareException("Migrations are happening for namespace '" + thisNamespace + "' on cluster 1, quick compare is not available.");
            }
            partitionMaps[i] = partitionMap;
        });
        if (!options.isSilent()) {
            System.out.printf("Quick record counts:\n");
            forEachCluster((i, c) -> {
                String thisNamespace = options.getNamespaceName(namespace, i);
                System.out.printf("\tcluster %d: (%d records, %d tombstones)\n",
                    i, partitionMaps[i].getRecordCount(thisNamespace), partitionMaps[i].getTombstoneCount(thisNamespace));
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

    private void differentRecords(int partitionId, Key key, DifferenceCollection differences, List<Integer> clustersWithRecord, RecordMetadata[] recordMetadatas) {
        for (RecordDifferenceHandler thisHandler : recordDifferenceHandlers) {
            try {
                thisHandler.handle(partitionId, key, differences, clustersWithRecord, recordMetadatas);
            }
            catch (Exception e) {
                System.err.printf("Error in %s: %s\n", thisHandler.getClass().getSimpleName(), e.getMessage());
                e.printStackTrace();
                this.forceTerminate = true;
            }
        }
    }

    private boolean isLutValid(RecordMetadata[] recordMetadatas, int clusterId) {
        return options.isDateInRange(recordMetadatas[clusterId].getLastUpdateMs());
    }
    
    private boolean filterMissingRecordsByMasterId(RecordMetadata[] recordMetadatas, List<Integer> clustersWithRecord, List<Integer> clustersWithoutRecord) {
        if (options.getMasterCluster() >= 0 && recordMetadatas != null) {
            if (clustersWithoutRecord.contains(options.getMasterCluster())) {
                // The master cluster has this record, check to see if the LUT is in the specified range.
                if (isLutValid(recordMetadatas, options.getMasterCluster())) {
                    // If the other records are out of range then ignore them.
                    for (Iterator<Integer> it = clustersWithoutRecord.iterator(); it.hasNext();) {
                        int clusterId = it.next();
                        if (!isLutValid(recordMetadatas, clusterId)) {
                            if (options.isDebug()) {
                                System.out.printf("Master cluster %s has record %s and is valid, cluster %s also has the record but the LUT (%s) is outside the range\n",
                                        options.clusterIdToName(options.getMasterCluster()),
                                        options.clusterIdToName(clusterId),
                                        options.getDateFormat().format(new Date(recordMetadatas[clusterId].getLastUpdateMs())));
                            }
                            it.remove();
                        }
                    }
                }
                else {
                    // The master record exists but is outside of the range, just ignore.
                    return false;
                }
            }
            else {
                //The master is missing. Only flag this as an issue if any other cluster has the record and is valid
                boolean hasValid = false;
                for (int clusterId : clustersWithRecord) {
                    if (isLutValid(recordMetadatas, clusterId)) {
                        hasValid = true;
                        break;
                    }
                }
                if (!hasValid) {
                    return false;
                }
            }
        }
        return true;
    }
    
    private void missingRecord(AerospikeClientAccess[] clients, List<Integer> clustersWithRecord, List<Integer> clustersWithoutRecord, 
            boolean hasRecordLevelDifferences, int partitionId, Key keyMissing) {
        RecordMetadata[] recordMetadatas = getMetadata(clients, keyMissing, clustersWithoutRecord, null);
        if (!filterMissingRecordsByMasterId(recordMetadatas, clustersWithRecord, clustersWithoutRecord)) {
            return;
        }
        for (MissingRecordHandler thisHandler : missingRecordHandlers) {
            try {
                thisHandler.handle(partitionId, keyMissing, clustersWithoutRecord, hasRecordLevelDifferences, recordMetadatas);
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
        
        // Set long timeouts and retries to allow for servers to fail and partition ownership to move over
        queryPolicy.totalTimeout = 30000;
        queryPolicy.sleepBetweenRetries = 2000;
        queryPolicy.maxRetries = 10;
        queryPolicy.socketTimeout = 2000;
        
        int rps = options.getRps()/this.threadsToUse;
        if (options.getRps() > 0 && rps == 0) {
            // Eg 10 threads, 5 rps would give 0
            rps = 1;
        }
        final int rpsThisThread = rps;

        Statement[] statements = new Statement[numberOfClusters];
        PartitionFilter[] filters = new PartitionFilter[clients.length];
        forEachCluster((i, c) -> {
            String namespaceName = options.getNamespaceName(namespace, i);
            Statement statement = new Statement();
            statement.setNamespace(namespaceName);
            statement.setSetName(setName);
            statement.setRecordsPerSecond(rpsThisThread);
            statements[i] = statement;
            filters[i] = PartitionFilter.id(partitionId);
        });

        if (options.isDebug()) {
            System.out.printf("Thread %d starting comparison of namespace %s, partition %d\n", Thread.currentThread().getId(), namespace, partitionId);
        }
        RecordSetAccess[] recordSets = new RecordSetAccess[clients.length];
        boolean[] sidesValid = new boolean[clients.length];
        for (int i = 0; i < clients.length; i++) {
            recordSets[i] = clients[i].queryPartitions(queryPolicy, statements[i], filters[i]);
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
                    System.out.printf("Part %d: next digest: %s, cluster digests:", partitionId, keyWithLargestDigest);
                    forEachCluster((i, c)-> System.out.printf(" %d:%s:%s", i, options.clusterIdToName(i), keys[i]));
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
                boolean hasRecordLevelDifferences = false;
                // Need to compare the records with the maximum digest
                if (options.isRecordLevelCompare()) {
                    DifferenceCollection result = compareRecords(comparator, partitionId, clustersWithMaxDigest, clients, recordSets, keys);
                    hasRecordLevelDifferences = result.hasDifferences();
                }
                if (!clustersWithoutRecord.isEmpty())  {
                    missingRecord(clients, clustersWithMaxDigest, clustersWithoutRecord, hasRecordLevelDifferences, partitionId, keyWithLargestDigest);
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
                    Record record1 = client1.isLocal() ? recordSet1.getRecord() : client1.get(tolerantReadPolicy, key1);
                    Record record2 = client2.isLocal() ? recordSet2.getRecord() : client2.get(tolerantReadPolicy, key2);
                    compareResult = comparator.compare(key1, record1, record2,
                            options.getPathOptions(), false, cluster1index, cluster2index);
                }
            }
        }
        differenceCollection.add(compareResult);
    }
    
    private Key getFirstNonNull(Key[] keys) {
        for (Key key : keys) {
            if (key != null) {
                return key;
            }
        }
        return null;
    }
    
    /**
     * Take a list of clusters and form the inverse of it. So the result will be an cluster which are not contained in the original cluster.
     * @param clusterList
     * @return a list of all clusters in the comparison which are not in the clusterList param
     */
    private List<Integer> invertClusterList(List<Integer> clusterList) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < numberOfClusters; i++) {
            if (!clusterList.contains(i)) {
                result.add(i);
            }
        }
        return result;
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
            RecordMetadata[] recordMetadatas = getMetadata(clients, getFirstNonNull(keys), null, clustersToCompare);
            // TODO: Master cluster logic
//            if (compareResult.filterByLastUpdateTimes(options, recordMetadatas)) {
                differentRecords(partitionId, getFirstNonNull(keys), compareResult, invertClusterList(clustersToCompare), recordMetadatas);
//            }
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
                        try {
                            comparePartition(clients, namespace, setName, partitionId);
                        }
                        catch (Exception e) {
                            // When a partition fails, it is unexpected as the retries
                            // have been set up to be very tolerant of timeouts, etc. 
                            // It would be possible to retry the partition by pushing it
                            // back onto the <code>partitionList</code> but then there is 
                            // a chance of infinite errors. Instead just flag it as an issue.
                            if (!options.isSilent()) {
                                System.out.printf("ERROR: Worker thread encountered an error scanning partition %d which prevented it from completing "
                                        + "the scan of the partition. The error encountered was %s (%s)\n", 
                                        partitionId, e.getMessage(), e.getClass());
                                e.printStackTrace();
                            }
                            synchronized (failedPartitionsList) {
                                failedPartitionsList.add(partitionId);
                            }
                        }
                    }
                }
            }
            finally {
                activeThreads.decrementAndGet();
            }
        }
    }
    
    private RecordMetadata[] getMetadata(AerospikeClientAccess[] clients, Key key, List<Integer> clustersToSkip, List<Integer> clustersToInclude) {
        RecordMetadata[] recordMetadatas = null;
        if (options.isShowMetadata() || options.getMasterCluster() >= 0) {
            recordMetadatas = new RecordMetadata[numberOfClusters];
            for (int i = 0; i < numberOfClusters; i++) {
                if ((clustersToSkip != null && clustersToSkip.contains(i)) || (clustersToInclude != null && !clustersToInclude.contains(i))) {
                    recordMetadatas[i] = null;
                }
                else {
                    Key namespaceResolvedKey = new Key(options.getNamespaceName(key.namespace, i), key.digest, key.setName, key.userKey);
                    recordMetadatas[i] = clients[i].getMetadata(tolerantWritePolicy, namespaceResolvedKey);
                }
            }
        }
        return recordMetadatas;
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
                            Key namespaceResolvedKey = line.getKey(i);
                            if (clients[i].exists(tolerantReadPolicy, namespaceResolvedKey)) {
                                clientsWithRecord.add(i);
                            }
                            else {
                                clientsWithoutRecord.add(i);
                            }
                        });
                        if (!clientsWithoutRecord.isEmpty()) {
                            missingRecord(clients, clientsWithRecord, clientsWithoutRecord, false, partId, key);
                        }
                    }
                    else {
                        // This must be a record level compare
                        DifferenceSet compareResult = null;
                        Record[] records = new Record[clients.length];
                        List<Integer> clustersWithRecord = new ArrayList<>();
                        List<Integer> clustersWithoutRecord = new ArrayList<>();
                        for (int i = 0; i < clients.length; i++) {
                            Key namespaceResolvedKey = line.getKey(i);
                            records[i] = clients[i].get(tolerantReadPolicy, namespaceResolvedKey);
                            if (records[i] == null) {
                                clustersWithoutRecord.add(i);
                            }
                            else {
                                clustersWithRecord.add(i);
                            }
                        }
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
                            RecordMetadata[] recordMetadatas = getMetadata(clients, key, clustersWithoutRecord, null);
                            // TODO: Master cluster logic
//                            if (differenceCollection.filterByLastUpdateTimes(options, recordMetadatas)) {
                                differentRecords(partId, key, differenceCollection, clustersWithoutRecord, recordMetadatas);
//                            }
                        }
                        if (!clustersWithoutRecord.isEmpty()) {
                            missingRecord(clients, clustersWithRecord, clustersWithoutRecord, 
                                    differenceCollection.hasDifferences(), partId, key);
                        }
                        long totalRecsCompared = totalRecordsCompared.incrementAndGet();
                        if (options.getRecordCompareLimit() > 0 && totalRecsCompared >= options.getRecordCompareLimit()) {
                            forceTerminate = true;
                        }
                    }
                    forEachCluster((i, c) -> recordsProcessedOnCluster.incrementAndGet(i));
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
            if (options.getPartitionList() != null) {
                this.partitionList = options.getPartitionList();
            }
            else {
                this.partitionList = IntStream.range(startPartition, endPartition).boxed().collect(Collectors.toList());
            }
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
    
    private void showSummary() {
        if (!options.isSilent()) {
            if (options.isRecordLevelCompare()) {
                forEachCluster((i, c) -> System.out.printf("Missing records on side %d : %,d\n", i+1, this.recordsMissingOnCluster.get(i)));
                System.out.printf("Records different         : %,d\n"
                                + "Records compared          : %,d\n", 
                        this.recordsDifferentCount.get(), this.totalRecordsCompared.get());
            }
            else {
                forEachCluster((i, c) -> System.out.printf("Missing records on side %d : %,d\n", i+1, this.recordsMissingOnCluster.get(i)));
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
            if (!this.failedPartitionsList.isEmpty()) {
                System.out.printf("******************************\n");
                System.out.printf("*                            *\n");
                System.out.printf("* WARNING: Failed Partitions *\n");
                System.out.printf("*                            *\n");
                System.out.printf("******************************\n");
                System.out.printf("Some partitions failed to successfully complete the scan. It is recommended that they "
                        + "be rescanned. This can be done by repeating this run and using the following flag:\n");
                System.out.printf("    --partitionList ");
                for (int i = 0; i < this.failedPartitionsList.size(); i++) {
                    if (i > 0) {
                        System.out.print(",");
                    }
                    System.out.printf("%d", failedPartitionsList.get(i));
                }
                System.out.println();
            }
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
                forEachCluster((i, c) -> System.out.printf("%s%s: %,d", i > 0 ? ", ": "" , options.clusterIdToName(i), currentRecordsForCluster[i]));
                System.out.printf("} throughput: {last second: %,d rps, overall: %,d rps}\n", 
                        recordsThisSecond/numberOfClusters,
                        (totalCurrentRecords)*1000/2/elapsedMilliseconds);
            }
            forEachCluster((i, c) -> lastRecordsForCluster[i] = currentRecordsForCluster[i]);
        }
        this.executor.awaitTermination(7, TimeUnit.DAYS);
        
        showSummary();
    }
    
    private void touchRecord(AerospikeClientAccess client, Key key) {
        try {
            client.touch(tolerantWritePolicy, key);
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
            client.get(tolerantReadPolicy, key);
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
            // It is possible for the producer to finish producing before a line is taken if the file is empty.
            // In that case, using <code>lines.take()</code> would wait forever. Hence <code>poll</code> is used
            // instead with a lenient timeout to resolve this race condition
            return lines.poll(10, TimeUnit.SECONDS);
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
                processor.process(clients, new FileLine(line, this.options));
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
    
    public void printExecutionParameters() {
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
            for (String namespace : this.options.getNamespaces()) {
                if (this.options.isNamespaceNameOverridden(namespace)) {
                    System.out.printf("  Namespace \"%s\" is known as", namespace);
                    boolean firstDifference = true;
                    for (int i = 0; i < numberOfClusters; i++) {
                        String thisClusterName = options.clusterIdToName(i);
                        String thisNamespace = options.getNamespaceName(namespace, i);
                        if (!thisNamespace.equals(namespace)) {
                            if (!firstDifference) {
                                System.out.print(", ");
                            }
                            firstDifference = false;
                            System.out.printf(" \"%s\" on cluster %s", thisNamespace, thisClusterName);
                        }
                    }
                    System.out.println();
                }
            }

            Date now = new Date();
            System.out.printf("Run starting at %s (%d) with comparison mode %s\n", options.getDateFormat().format(now), now.getTime(), options.getCompareMode());
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
        this.printExecutionParameters();
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

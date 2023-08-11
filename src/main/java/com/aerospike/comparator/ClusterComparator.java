package com.aerospike.comparator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.AerospikeException.InvalidNode;
import com.aerospike.client.cluster.ClusterUtilities;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
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
import com.aerospike.comparator.dbaccess.RemoteUtils;

public class ClusterComparator {

    // TODO:
    // - config file to remove certain bins from comparison of sets
    // - Protobuf'ing binary fields
    // - Or msgpack
    // - Compare metadata -- sindex, set indexes, etc.
    // - Ability to compare different sets (scan/batch comparator) Note: how to ensure it's not just a one-way comparison?
    
    private final int startPartition;
    private final int endPartition;
    private final AtomicLong recordsCluster1Processed = new AtomicLong();
    private final AtomicLong recordsCluster2Processed = new AtomicLong();
    private final AtomicLong missingRecordsCluster1 = new AtomicLong();
    private final AtomicLong missingRecordsCluster2 = new AtomicLong();
    private final AtomicLong recordsDifferentCount = new AtomicLong();
    private final AtomicLong totalMissingRecords = new AtomicLong();
    private final AtomicLong totalRecordsCompared = new AtomicLong();
    
    private ExecutorService executor = null;
    private AtomicInteger activeThreads;
    private volatile boolean forceTerminate = false;
    private final List<MissingRecordHandler> missingRecordHandlers = new ArrayList<>();
    private final List<RecordDifferenceHandler> recordDifferenceHandlers = new ArrayList<>();
    private final ClusterComparatorOptions options;
    private int threadsToUse;
    private List<Integer> partitionList = new ArrayList<>();
    private Expression filterExpresion = null;
    
    private class InternalHandler implements MissingRecordHandler, RecordDifferenceHandler {
        private void checkDifferencesCount() {
            if (options.getMissingRecordsLimit() > 0 && (totalMissingRecords.get() + recordsDifferentCount.get() >= options.getMissingRecordsLimit())) {
                forceTerminate = true;
            }
        }
        
        @Override
        public void handle(int partitionId, Key key, Side missingFromSide) throws IOException {
            if (missingFromSide == Side.SIDE_1) {
                missingRecordsCluster1.incrementAndGet();
            }
            else {
                missingRecordsCluster2.incrementAndGet();
            }
            totalMissingRecords.incrementAndGet();
            checkDifferencesCount();
        }

        @Override
        public void handle(int partitionId, Key key, Record side1, Record side2, DifferenceSet differences)
                throws IOException {
            recordsDifferentCount.incrementAndGet();
            checkDifferencesCount();
        }
    }
        
    public ClusterComparator(ClusterComparatorOptions options) throws IOException {
        this.options = options;
        this.startPartition = Math.max(options.getStartPartition(), 0);
        this.endPartition = Math.min(4096, options.getEndPartition());
        InternalHandler handler = new InternalHandler();
        this.missingRecordHandlers.add(handler);
        this.recordDifferenceHandlers.add(handler);
        if (options.isConsole()) {
            ConsoleDifferenceHandler consoleHandler = new ConsoleDifferenceHandler();
            this.missingRecordHandlers.add(consoleHandler);
            this.recordDifferenceHandlers.add(consoleHandler);
        }
        if (options.getFileName() != null && options.getAction() != Action.TOUCH && options.getAction() != Action.READ) {
            CsvDifferenceHandler csvHandler = new CsvDifferenceHandler(options.getFileName());
            this.missingRecordHandlers.add(csvHandler);
            this.recordDifferenceHandlers.add(csvHandler);
        }
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
    
    public AerospikeClientAccess connectClient(Side side) {
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.user = (side == Side.SIDE_1) ? options.getUserName1() : options.getUserName2();
        clientPolicy.password = (side == Side.SIDE_1) ? options.getPassword1() : options.getPassword2();
        clientPolicy.tlsPolicy = (side == Side.SIDE_1) ? options.getTlsPolicy1() : options.getTlsPolicy2();
        clientPolicy.authMode = (side == Side.SIDE_1) ? options.getAuthMode1() : options.getAuthMode2();
        clientPolicy.clusterName = (side == Side.SIDE_1) ? options.getClusterName1() : options.getClusterName2();
        clientPolicy.useServicesAlternate = (side == Side.SIDE_1) ? options.isServicesAlternate1() : options.isServicesAlternate2();
        String hostNames = (side == Side.SIDE_1) ? options.getHosts1() : options.getHosts2();
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
                            side.value, hostNames, tlsPolicyAsString(clientPolicy.tlsPolicy));
                }
                return new RemoteAerospikeClient(remoteHost[1], Integer.valueOf(remoteHost[2]), this.threadsToUse, options.getRemoteCacheSize(), clientPolicy.tlsPolicy);
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
                    char[] pass = console.readPassword("Enter password for cluster " + ((side == Side.SIDE_1) ? "1" : "2") +  ": ");
                    if (pass != null) {
                        clientPolicy.password = new String(pass);
                    }
                }
            }
            IAerospikeClient client = new AerospikeClient(clientPolicy, hosts);
            if (!options.isSilent()) {
                System.out.printf("Cluster %d: name: %s, hosts: %s user: %s, password: %s\n", side.value, 
                        clientPolicy.clusterName, Arrays.toString(hosts), clientPolicy.user, clientPolicy.password == null ? "null" : "********");
                System.out.printf("         authMode: %s, tlsPolicy: %s\n", clientPolicy.authMode, tlsPolicyAsString(clientPolicy.tlsPolicy));
                new ClusterUtilities(client).printInfo(true, 120);
            }
            return new LocalAerospikeClient(client);
        }
    }

    private List<Integer> quickCompare(AerospikeClientAccess client1, AerospikeClientAccess client2, String namespace) {
        PartitionMap partitionMap1 = new PartitionMap(client1);
        PartitionMap partitionMap2 = new PartitionMap(client2);
        if (!partitionMap1.isComplete(namespace)) {
            throw new QuickCompareException("Not all partitions are available for namespace '" + namespace + "' on cluster 1, quick compare is not available.");
        }
        if (!partitionMap2.isComplete(namespace)) {
            throw new QuickCompareException("Not all partitions are available for namespace '" + namespace + "' on cluster 2, quick compare is not available.");
        }
        if (partitionMap1.isMigrationsHappening(namespace)) {
            throw new QuickCompareException("Migrations are happening for namespace '" + namespace + "' on cluster 1, quick compare is not available.");
        }
        if (partitionMap2.isMigrationsHappening(namespace)) {
            throw new QuickCompareException("Migrations are happening for namespace '" + namespace + "' on cluster 2, quick compare is not available.");
        }
        if (!options.isSilent()) {
            System.out.printf("Quick record counts: cluster 1: (%d records, %d tombstones), cluster 2: (%d records, %d tombstones)\n",
                    partitionMap1.getRecordCount(namespace), partitionMap1.getTombstoneCount(namespace),
                    partitionMap2.getRecordCount(namespace), partitionMap2.getTombstoneCount(namespace));
        }
        List<Integer> partitionList = partitionMap1.compare(partitionMap2, namespace);
        return partitionList;
    }
    
    private int compare(Key key1, Key key2) {
        if (key1 == null && key2 == null) {
            // This should never happen
            return 0;
        }
        else if (key1 == null) {
            return -1;
        }
        else if (key2 == null) {
            return 1;
        }
        byte[] digest1 = key1.digest;
        byte[] digest2 = key2.digest;
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
    
    private void differentRecords(int partitionId, Key key, Record record1, Record record2, DifferenceSet differences) {
        for (RecordDifferenceHandler thisHandler : recordDifferenceHandlers) {
            try {
                thisHandler.handle(partitionId, key, record1, record2, differences);
            }
            catch (Exception e) {
                System.err.printf("Error in %s: %s\n", thisHandler.getClass().getSimpleName(), e.getMessage());
                e.printStackTrace();
                this.forceTerminate = true;
            }
        }
    }
    private void missingRecord(AerospikeClientAccess clientWithKey, int partitionId, Key key, Side missingFromSide) {
        for (MissingRecordHandler thisHandler : missingRecordHandlers) {
            try {
                thisHandler.handle(partitionId, key, missingFromSide);
            }
            catch (Exception e) {
                System.err.printf("Error in %s: %s\n", thisHandler.getClass().getSimpleName(), e.getMessage());
                e.printStackTrace();
                this.forceTerminate = true;
            }
        }
        if (this.options.getAction() == Action.SCAN_TOUCH) {
            touchRecord(clientWithKey, key);
        }
        if (this.options.getAction() == Action.SCAN_READ) {
            readRecord(clientWithKey, key);
        }
    }
    
    private boolean getNextRecord(RecordSetAccess recordSet, Side side) {
        boolean result = recordSet.next();
        if (result) {
            switch (side) {
            case SIDE_1: 
                recordsCluster1Processed.incrementAndGet();
                break;
                
            case SIDE_2:
                recordsCluster2Processed.incrementAndGet();
            }
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
                    Exp.ge(Exp.lastUpdate(), Exp.val(TimeUnit.MILLISECONDS.toNanos(this.options.getEndDate().getTime())))));
        }
    }
    
    private void comparePartition(AerospikeClientAccess client1, AerospikeClientAccess client2, String namespace, String setName, int partitionId) {
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
        PartitionFilter filter1 = PartitionFilter.id(partitionId);
        PartitionFilter filter2 = PartitionFilter.id(partitionId);
        
        RecordSetAccess recordSet1 = client1.queryPartitions(queryPolicy, statement, filter1);
        RecordSetAccess recordSet2 = client2.queryPartitions(queryPolicy, statement, filter2);
        boolean side1Valid = getNextRecord(recordSet1, Side.SIDE_1);
        boolean side2Valid = getNextRecord(recordSet2, Side.SIDE_2);

        RecordComparator comparator = new RecordComparator();

        try {
            while ((side1Valid || side2Valid) && !forceTerminate) {

                Key key1 = side1Valid ? recordSet1.getKey() : null;
                Key key2 = side2Valid ? recordSet2.getKey() : null;
                int result = compare(key1, key2);
                if (result < 0) {
                    // The digests go down as we go through the partition, so if side 2 is > side 1
                    // it means that side 1 has missed this one and we need to advance side2
                    missingRecord(client2, partitionId, key2, Side.SIDE_1);
                    side2Valid = getNextRecord(recordSet2, Side.SIDE_2);
                }
                else if (result > 0) {
                    // The digests go down as we go through the partition, so if side 1 is > side 2
                    // it means that side 2 has missed this one and we need to advance side1
                    missingRecord(client1, partitionId, key1, Side.SIDE_2);
                    side1Valid = getNextRecord(recordSet1, Side.SIDE_1);
                }
                else {
                    if (options.isRecordLevelCompare()) {
                        Record record1 = recordSet1.getRecord();
                        Record record2 = recordSet2.getRecord();
                        DifferenceSet compareResult = comparator.compare(key1, record1, record2,
                                options.getPathOptions(),
                                options.getCompareMode() == CompareMode.RECORDS_DIFFERENT);
                        if (compareResult.areDifferent()) {
                            differentRecords(partitionId, key2, null, null, compareResult);
                        }
                        if (options.getRecordCompareLimit() > 0 && totalRecordsCompared.incrementAndGet() >= options.getRecordCompareLimit()) {
                            forceTerminate = true;
                        }
                    }
                    // The keys are equal, move on.
                    side1Valid = getNextRecord(recordSet1, Side.SIDE_1);
                    side2Valid = getNextRecord(recordSet2, Side.SIDE_2);
                }
            }
        }
        finally {
            recordSet1.close();
            recordSet2.close();
        }
    }

    private void beginComparison(AerospikeClientAccess client1, AerospikeClientAccess client2, String namespace, String setName) throws InterruptedException {
        recordsCluster1Processed.set(0);
        recordsCluster2Processed.set(0);
        missingRecordsCluster1.set(0);
        missingRecordsCluster2.set(0);

        List<Integer> partitionsToCompare; 
        if (options.isQuickCompare()) {
            try {
                partitionsToCompare = quickCompare(client1, client2, namespace);
                if (!options.isSilent()) {
                    if (partitionsToCompare.size() <= 20) {
                        System.out.printf("Quick compare found %d partitions different: %s\n", partitionsToCompare.size(), partitionsToCompare);
                    }
                    else {
                        System.out.printf("Quick compare found %d partitions different\n", partitionsToCompare.size());
                    }
                }
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
        else {
            partitionsToCompare = IntStream.range(startPartition, endPartition).boxed().collect(Collectors.toList());
        }
        
        this.partitionList = partitionsToCompare;
        this.executor = Executors.newFixedThreadPool(threadsToUse);
        this.activeThreads = new AtomicInteger(threadsToUse);
        for (int i = 0; i < threadsToUse; i++) {
            this.executor.execute(() -> {
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
                            comparePartition(client1, client2, namespace, setName, partitionId);
                        }
                    }
                }
                finally {
                    this.activeThreads.decrementAndGet();
                }
            });
        }
        this.executor.shutdown();
        this.monitorProgress(namespace, setName);
    }
    
    private void beginComparisons(AerospikeClientAccess client1, AerospikeClientAccess client2) throws InterruptedException {
        this.filterExpresion = formFilterExpression();

        if (options.isMetadataCompare()) {
            MetadataComparator metadataComparator = new MetadataComparator(options);
            DifferenceSet result = metadataComparator.compareMetaData(client1, client2);
        }
        for (String namespace : options.getNamespaces()) {
            String[] sets = options.getSetNames();
            if (sets == null || sets.length == 0) {
                beginComparison(client1, client2, namespace, null);
            }
            else {
                for (String thisSet : sets) {
                    beginComparison(client1, client2, namespace, thisSet);
                }
            }
        }
        for (MissingRecordHandler thisHandler : missingRecordHandlers) {
            thisHandler.close();
        }
    }
    
    private void monitorProgress(String namespace, String setName) throws InterruptedException {
        if (!options.isSilent()) {
            System.out.println("Comparison started for namespace " + namespace + ((setName == null) ?  "." : (", set " + setName + ".")));
        }
        long lastRecordsCluster1 = 0;
        long lastRecordsCluster2 = 0;
        long startTime = System.currentTimeMillis();
        while (activeThreads.get() > 0) {
            Thread.sleep(1000);
            long currentRecordsCluster1 = this.recordsCluster1Processed.get();
            long currentRecordsCluster2 = this.recordsCluster2Processed.get();
            int nextPartition = this.partitionList.size();
            int activeThreads = this.activeThreads.get();
            long now = System.currentTimeMillis();
            long elapsedMilliseconds = now - startTime;
            if (!options.isSilent()) {
                System.out.printf("%,dms: [%d-%d, remaining %d], active threads: %d, records processed: {cluster1: %,d, cluster2: %,d} throughput: {last second: %,d rps, overall: %,d rps}\n", 
                        (now-startTime), this.startPartition, this.endPartition, nextPartition, activeThreads, currentRecordsCluster1, 
                        currentRecordsCluster2, ((currentRecordsCluster1-lastRecordsCluster1)+(currentRecordsCluster2-lastRecordsCluster2))/2,
                        (currentRecordsCluster1+currentRecordsCluster2)*1000/2/elapsedMilliseconds);
            }
            lastRecordsCluster1 = currentRecordsCluster1;
            lastRecordsCluster2 = currentRecordsCluster2;
        }
        this.executor.awaitTermination(7, TimeUnit.DAYS);
        
        if (!options.isSilent()) {
            if (options.isRecordLevelCompare()) {
                System.out.printf("Finished: %d missing records on side 1, %d missing records on side 2, %d records different, %,d records compared.\n", 
                        this.missingRecordsCluster1.get(), this.missingRecordsCluster2.get(), this.recordsDifferentCount.get(), this.totalRecordsCompared.get());
            }
            else {
                System.out.printf("Finished: %,d missing records on side 1, %,d missing records on side 2.\n", 
                        this.missingRecordsCluster1.get(), this.missingRecordsCluster2.get());
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
    
    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                                 + Character.digit(s.charAt(i+1), 16));
        }
        return data;
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

    private void touchRecord(AerospikeClientAccess client, String namespace, String setName, String digest) {
        Key key = new Key(namespace, hexStringToByteArray(digest), setName, null);
        touchRecord(client, key);
    }
    
    private void readRecord(AerospikeClientAccess client, String namespace, String setName, String digest) {
        Key key = new Key(namespace, hexStringToByteArray(digest), setName, null);
        readRecord(client, key);
    }
    
    private void processRecords(AerospikeClientAccess client1, AerospikeClientAccess client2, boolean touch, boolean read) throws IOException {
        File file = new File(options.getFileName());
        BufferedReader br = new BufferedReader(new FileReader(file));
        
        try {
            String line = br.readLine();
            if (!CsvDifferenceHandler.FILE_HEADER.equals(line)) {
                throw new UnsupportedOperationException("File " + options.getFileName() + " has a header which does not match what was expected. Expected '" + CsvDifferenceHandler.FILE_HEADER + "' but received '" + line + "'");
            }

            while ((line = br.readLine()) != null) {
                String[] linePart = line.split(",");
                String namespace = linePart[0];
                String setName = linePart[1];
                String userKey = linePart[2];
                String digest1 = linePart[3];
                String digest2 = linePart.length >= 5 ? linePart[4] : null;
                if (digest1 != null && !digest1.trim().isEmpty() && !"null".equals(digest1)) {
                    if (read) {
                        this.readRecord(client1, namespace, setName, digest1);
                    }
                    if (touch) {
                        this.touchRecord(client1, namespace, setName, digest1);
                    }
                }
                else if (digest2 != null && !digest2.trim().isEmpty()) {
                    if (read) {
                        this.readRecord(client2, namespace, setName, digest2);
                    }
                    if (touch) {
                        this.touchRecord(client2, namespace, setName, digest2);
                    }
                }
            }
        } finally {
            br.close();
        }
        
    }
    
    private void startRemoteServer() {
        AerospikeClientAccess client1 = this.connectClient(Side.SIDE_1);
        RemoteServer remoteServer = new RemoteServer(client1, options.getRemoteServerPort(), options.getRemoteServerHeartbeatPort());
        try {
            remoteServer.start(options.getRemoteServerTls());
        } catch (IOException e) {
            System.err.printf("IOException occurred in remote server mode, terminating server. %s\n", e.getMessage());
            e.printStackTrace();
        }
    }
    
    public DifferenceSummary begin() throws Exception {
        if (options.isRemoteServer()) {
            startRemoteServer();
            return null;
        }
        if (!options.isSilent()) {
            System.out.printf("Beginning scan with namespaces '%s', sets '%s', start partition: %d, end partition %d\n", 
                    String.join(",", this.options.getNamespaces()),
                    this.options.getSetNames() == null ? "null" : String.join(",", this.options.getSetNames()),
                    this.options.getStartPartition(), this.options.getEndPartition());
            Date now = new Date();
            System.out.printf("Run starting at %s (%d) with comparison mode %s\n", options.getDateFormat().format(now), now.getTime(), options.getCompareMode());
        }
        this.threadsToUse = options.getThreads() <= 0 ? Runtime.getRuntime().availableProcessors() : options.getThreads();
        AerospikeClientAccess client1 = this.connectClient(Side.SIDE_1);
        AerospikeClientAccess client2 = this.connectClient(Side.SIDE_2);
//    System.out.println(client1.getNodeNames());
//    System.out.println(client2.getNodeNames());
//    System.exit(0);
        Scanner input = null;
        try {
            if (options.getAction() == Action.TOUCH) {
                this.processRecords(client1, client2, true, false);
            }
            else if (options.getAction() == Action.READ) {
                this.processRecords(client1, client2, false, true);
            }
            else {
                this.beginComparisons(client1, client2);
                if (options.getAction() == Action.SCAN_ASK && (totalMissingRecords.get() > 0 || recordsDifferentCount.get() > 0)) {
                    System.out.printf("%,d differences found between the 2 clusters. Do you want those records to be touched, read or none? (t/r//N)", totalMissingRecords.get());
                    input = new Scanner(System.in);
                    boolean touch = false;
                    boolean read = false;
                    while (true) {
                        String  next = input.nextLine();
                        if (next.trim().toUpperCase().startsWith("T")) {
                            touch = true;
                            break;
                        }
                        if (next.trim().toUpperCase().startsWith("R")) {
                            read = true;
                            break;
                        }
                        if (next.trim().toUpperCase().startsWith("N") || next.trim().length() == 0) {
                            break;
                        }
                        System.out.printf("Please enter 't', 'r' or 'n'\n");
                    }
                    if (touch || read) {
                        processRecords(client1, client2, touch, read);
                    }
                }
            }
        }
        finally {
            client1.close();
            client2.close();
            if (input != null) {
                input.close();
            }
        }
        return new DifferenceSummary(missingRecordsCluster1.get(), missingRecordsCluster2.get(), recordsDifferentCount.get());
    }
    
    public static void main(String[] args) throws Exception {
        ClusterComparatorOptions options = new ClusterComparatorOptions(args);
        
        ClusterComparator comparator = new ClusterComparator(options);
        comparator.begin();
        
    }
}

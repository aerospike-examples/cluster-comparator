package com.aerospike.comparator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import com.aerospike.client.Key;
import com.aerospike.client.command.Buffer;
import com.aerospike.comparator.DifferenceCollection.RecordDifferences;
import com.aerospike.comparator.dbaccess.RecordMetadata;

public class CsvDifferenceHandler implements MissingRecordHandler, RecordDifferenceHandler {
    public final String FILE_HEADER;
    private final File file;
    private PrintWriter writer;
    private final int numberOfClusters;
    private final ClusterComparatorOptions options;

    public CsvDifferenceHandler(String fileName, ClusterComparatorOptions options) throws IOException {
        if (fileName != null) {
            this.file = new File(fileName);
            writer = new PrintWriter(new FileWriter(file));
        }
        else {
            this.file = null;
        }
        this.options = options;
        this.numberOfClusters = options.getClusterConfigs().size();
        StringBuilder sb = new StringBuilder().append("Namespace,Set,Partition,Key,");
        if (numberOfClusters != 2) {
            sb.append("Number of Clusters").append(',');
        }
        for (int i = 0; i < numberOfClusters; i++) {
            sb.append("Digest - ").append(options.clusterIdToName(i)).append(',');
            if (options.isShowMetadata()) {
                String clusterName = options.clusterIdToName(i);
                sb.append("LUT - ").append(clusterName).append(',');
                sb.append("size - ").append(clusterName).append(',');
                sb.append("Gen - ").append(clusterName).append(',');
                sb.append("TTL - ").append(clusterName).append(',');
            }
        }
        sb.append("Diffs");
        this.FILE_HEADER = sb.toString();

        writer.println(FILE_HEADER);
        writer.flush();
    }
    
    public String getFileHeader() {
        return this.FILE_HEADER;
    }

    private String csvify(String s) {
        if (s == null) {
            return "";
        }
        if (!s.contains("\"")) {
            return s;
        }
        return "\"" + s.replace("\"", "\"\"") + "\"";
    }
    
    @Override
    public synchronized void handle(int partitionId, Key key, List<Integer> missingFromClusters, RecordMetadata[] metadatas) throws IOException {
        String missingClusters = missingFromClusters.stream().map(id->options.clusterIdToName(id)).collect(Collectors.toList()).toString();
        String humanlyReadable = "Missing from clusters: " + missingClusters;
        String json = "{\"MISSING\":" + missingClusters + "}";
        writeDifference(partitionId, key, missingFromClusters, metadatas, json, humanlyReadable);
    }

    private void writeDifference(int partitionId, Key key, List<Integer> missingFromClusters, RecordMetadata[] metadatas, String ...differences) {
        StringBuilder sb = new StringBuilder();
        sb.append(key.namespace).append(',')
            .append(key.setName).append(',')
            .append(partitionId).append(',')
            .append(key.userKey).append(',');
        
        if (numberOfClusters != 2) {
            sb.append(numberOfClusters).append(',');
        }
        String digest = Buffer.bytesToHexString(key.digest);
        for (int i = 0; i < numberOfClusters; i++) {
            if (missingFromClusters == null || !missingFromClusters.contains(i)) {
                sb.append(digest);
            }
            else {
                sb.append("");
            }
            sb.append(',');
            if (metadatas != null) {
                if (metadatas[i] == null) {
                    sb.append(",,,,");
                }
                else {
                    sb.append(options.getDateFormat().format(new Date(metadatas[i].getLastUpdateMs())))
                      .append(',')
                      .append(metadatas[i].getRecordSize())
                      .append(',')
                      .append(metadatas[i].getGeneration())
                      .append(',')
                      .append(metadatas[i].getTtl())
                      .append(',');
                }
            }
        }
        for (String s : differences) {
            sb.append(csvify(s)).append(',');
        }
        writer.print(sb.append('\n').toString());
        writer.flush();
    }
    @Override
    public synchronized void handle(int partitionId, Key key, DifferenceCollection differences, RecordMetadata[] metadatas) throws IOException {
        
        if (options.isBinsOnly()) {
            RecordDifferences differencesOnRecord = differences.getBinsDifferent();
            writeDifference(partitionId, key, null, metadatas,
                    differencesOnRecord.toRawString(options),
                    differencesOnRecord.toHumanString(options));
        }
        else {
            // Do not show the whole binary blob
            // We have to manipulate this to make it valid CSV. Any double quotes become
            // double double quote, then put the whole thing in double quotes.
            for (DifferenceSet diffSet : differences.getDifferenceSets()) {
                writeDifference(partitionId, key, null, metadatas, diffSet.getAsJson(true, this.options).replaceAll("\"", "\"\""));
            }
        }
    }

    @Override
    public void close() {
        writer.close();
    }
}

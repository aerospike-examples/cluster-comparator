package com.aerospike.comparator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.stream.Collectors;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.command.Buffer;
import com.aerospike.comparator.DifferenceCollection.RecordDifferences;

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
        StringBuilder sb = new StringBuilder().append("Namespace,Set,Key,");
        if (numberOfClusters != 2) {
            sb.append("Number of Clusters").append(',');
        }
        for (int i = 0; i < numberOfClusters; i++) {
            sb.append("Digest - ").append(options.clusterIdToName(i)).append(',');
        }
        this.FILE_HEADER = sb.append("Diffs").toString();

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
    public synchronized void handle(int partitionId, Key key, List<Integer> missingFromClusters) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(key.namespace).append(',')
          .append(key.setName).append(',')
          .append(key.userKey).append(',');
        if (numberOfClusters != 2) {
            sb.append(numberOfClusters).append(',');
        }
        for (int i=0; i < numberOfClusters; i++) {
            sb.append(missingFromClusters.contains(i) ? "" : Buffer.bytesToHexString(key.digest)).append(',');
        }
        sb.append("Missing from clusters: ").append(missingFromClusters.stream().map(i->options.clusterIdToName(i)).collect(Collectors.toList())).append('\n');
        writer.print(sb.toString());
        writer.flush();
    }

    private void writeDifference(Key key, String ...differences) {
        StringBuilder sb = new StringBuilder();
        sb.append(key.namespace).append(',')
            .append(key.setName).append(',')
            .append(key.userKey).append(',');
        
        if (numberOfClusters != 2) {
            sb.append(numberOfClusters).append(',');
        }
        String digest = Buffer.bytesToHexString(key.digest);
        for (int i = 0; i < numberOfClusters; i++) {
            sb.append(digest).append(',');
        }
        for (String s : differences) {
            sb.append(s).append(',');
        }
        writer.print(sb.append('\n').toString());
        writer.flush();
        
    }
    @Override
    public synchronized void handle(int partitionId, Key key, Record side1, Record side2, DifferenceCollection differences)
            throws IOException {
        
        if (options.isBinsOnly()) {
            RecordDifferences differencesOnRecord = differences.getBinsDifferent(key);
            writeDifference(key, 
                    csvify(differencesOnRecord.toRawString(options)),
                    csvify(differencesOnRecord.toHumanString(options)));
        }
        else {
            // Do not show the whole binary blob
            // We have to manipulate this to make it valid CSV. Any double quotes become
            // double double quote, then put the whole thing in double quotes.
            for (DifferenceSet diffSet : differences.getDifferenceSets()) {
                writeDifference(key, diffSet.getAsJson(true, this.options).replaceAll("\"", "\"\""));
            }
        }
    }

    @Override
    public void close() {
        writer.close();
    }

}

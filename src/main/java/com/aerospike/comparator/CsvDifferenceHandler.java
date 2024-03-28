package com.aerospike.comparator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.command.Buffer;

public class CsvDifferenceHandler implements MissingRecordHandler, RecordDifferenceHandler {
    public final String FILE_HEADER;
    private final File file;
    private PrintWriter writer;
    private final int numberOfClusters;

    public CsvDifferenceHandler(String fileName, int numberOfClusters) throws IOException {
        if (fileName != null) {
            this.file = new File(fileName);
            writer = new PrintWriter(new FileWriter(file));
        }
        else {
            this.file = null;
        }
        this.numberOfClusters = numberOfClusters;
        StringBuffer sb = new StringBuffer().append("Namespace,Set,Key,");
        for (int i = 0; i < numberOfClusters; i++) {
            sb.append("Digest").append(i+1).append(',');
        }
        this.FILE_HEADER = sb.append("Diffs").toString();
    }
    
    public String getFileHeader() {
        return this.FILE_HEADER;
    }

    @Override
    public synchronized void handle(int partitionId, Key key, List<Integer> missingFromClusters) throws IOException {
        StringBuffer sb = new StringBuffer();
        sb.append(key.namespace).append(',')
          .append(key.setName).append(',')
          .append(key.userKey).append(',');
        if (numberOfClusters != 2) {
            sb.append(numberOfClusters).append(',');
        }
        for (int i=0; i < numberOfClusters; i++) {
            sb.append(missingFromClusters.contains(i) ? "" : Buffer.bytesToHexString(key.digest));
        }
        sb.append("Missing from clusters: ").append(missingFromClusters);
        writer.print(sb.toString());
        writer.flush();
    }

    @Override
    public synchronized void handle(int partitionId, Key key, Record side1, Record side2, DifferenceSet differences)
            throws IOException {
        String differencesString = differences.getAsJson();
        // We have to manipulate this to make it valid CSV. Any double quotes become
        // double double quote, then put the whole thing in double quotes.
        differencesString = differencesString.replaceAll("\"", "\"\"");
        writer.printf("%s,%s,%s,%s,%s,\"%s\"\n", key.namespace, key.setName, key.userKey,
                Buffer.bytesToHexString(key.digest), Buffer.bytesToHexString(key.digest), differencesString);
        writer.flush();
    }

    @Override
    public void close() {
        writer.close();
    }

}

package com.aerospike.comparator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.command.Buffer;

public class CsvDifferenceHandler implements MissingRecordHandler, RecordDifferenceHandler {
    public static final String FILE_HEADER = "Namespace,Set,Key,Digest1,Digest2,Diffs";
    private File file;
    private PrintWriter writer;

    public CsvDifferenceHandler(String fileName) throws IOException {
        this.file = new File(fileName);
        writer = new PrintWriter(new FileWriter(file));
        writer.println(FILE_HEADER);
    }

    @Override
    public synchronized void handle(int partitionId, Key key, Side missingFromSide) throws IOException {
        String side1Digest = missingFromSide == Side.SIDE_1 ? "" : Buffer.bytesToHexString(key.digest);
        String side2Digest = missingFromSide == Side.SIDE_2 ? "" : Buffer.bytesToHexString(key.digest);
        writer.printf("%s,%s,%s,%s,%s,Side %d missing\n", key.namespace, key.setName, key.userKey, side1Digest, side2Digest,
                missingFromSide.value);
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

package com.aerospike.comparator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;

public class AbstractBaseTest {
    public static final int NUM_CLUSTERS = 3;
    private final String[] hosts = new String[NUM_CLUSTERS];
    public static final String SET_NAME = "compTestSet";
    
   private final IAerospikeClient[] clients = new IAerospikeClient[NUM_CLUSTERS];
    public AbstractBaseTest() {
        hosts[0] = "localhost:3100";
        hosts[1] = "localhost:3101";
        hosts[2] = "localhost:3102";
        for (int i = 0; i < NUM_CLUSTERS; i++) {
            clients[i] = new AerospikeClient(null, Host.parseHosts(hosts[i], 3000));
        }
    }
    
    protected String getHostString(int clusterId) {
        if (clusterId < 0 || clusterId >= NUM_CLUSTERS) {
            throw new IllegalArgumentException(String.format("clusterId must be between 0 and %d, not %d", clients.length-1, clusterId));
        }
        return hosts[clusterId];
    }
    protected IAerospikeClient getClient(int clusterId) {
        if (clusterId < 0 || clusterId >= NUM_CLUSTERS) {
            throw new IllegalArgumentException(String.format("clusterId must be between 0 and %d, not %d", clients.length-1, clusterId));
        }
        return clients[clusterId];
    }
    
    private String writeToFile(String pFilename, String data) throws IOException {
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        File tempFile = File.createTempFile(pFilename, ".tmp", tempDir);
        String result = tempFile.getAbsolutePath();
        FileWriter fileWriter = new FileWriter(tempFile, true);
        System.out.println(tempFile.getAbsolutePath());
        BufferedWriter bw = new BufferedWriter(fileWriter);
        bw.write(data);
        bw.close();
        return result;
    }
    
    protected boolean removeFile(String filename) {
        return new File(filename).delete();
    }
    
    protected String writeYamlToFile(String fileName, String ... lines) throws IOException {
        String newLine = System.getProperty("line.separator");
        String linesToWrite = String.join(newLine, lines);
        return writeToFile(fileName, linesToWrite);
    }
}

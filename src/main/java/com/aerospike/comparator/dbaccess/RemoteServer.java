package com.aerospike.comparator.dbaccess;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.Statement;

public class RemoteServer {
    public static final int CMD_CLOSE = 1;
    public static final int CMD_TOUCH = 2;
    public static final int CMD_GET = 3;
    public static final int CMD_QUERY_PARTITION = 4;
    public static final int CMD_INVOKE_INFO_CMD_ON_ALL_NODES = 5;
    public static final int CMD_INVOKE_INFO_CMD_ON_A_NODE = 6;
    public static final int CMD_GET_NODE_NAMES = 7;
    public static final int CMD_RS_NEXT = 8;
    public static final int CMD_RS_KEY = 9;
    public static final int CMD_RS_RECORD = 10;
    public static final int CMD_RS_CLOSE = 11;
    public static final int CMD_RS_MULTI = 12;
    public static final int CMD_RS_RECORD_HASH = 13;
    public static final int CMD_RS_MULTI_RECORD_HASH = 14;
    
    private final boolean debug;
    private final boolean verbose;
    private final AerospikeClientAccess client;
    private final int port;
    
    public RemoteServer(final AerospikeClientAccess client, final int port, final int heartbeatPort, final boolean verbose, final boolean debug) {
        this.client = client;
        this.port = port;
        this.verbose = verbose;
        this.debug = debug;
        if (heartbeatPort > 0) {
            this.startHeartbeatServer(heartbeatPort);
        }
    }
    
    private void startHeartbeatServer(int heartbeatPort) {
        Thread heartbeatThread = new Thread(() -> {
            ServerSocket serverSocket = null;
            try {
                serverSocket = new ServerSocket(heartbeatPort);
                System.out.printf("Server heartbeat listening on port %d\n", heartbeatPort);
            }
            catch (IOException ioe) {
                System.err.printf("Error creating heartbeat socket on port %d\n", heartbeatPort);
            }
            while (true) {
                Socket socket = null;
                DataInputStream dis = null;
                DataOutputStream dos = null;
                try {
                    socket = serverSocket.accept();
                    dis = new DataInputStream(socket.getInputStream());
                    dos = new DataOutputStream(socket.getOutputStream());
                    while (true) {
                        byte b = dis.readByte();
                        dos.writeByte(b);
                    }
                }
                catch (IOException ioe) {
                    if (dis != null) {
                        try {
                            dis.close();
                        } catch (IOException ignored) {}
                    }
                    if (dos != null) {
                        try {
                            dos.close();
                        } catch (IOException ignored) {}
                    }
                    if (socket != null) {
                        try {
                            socket.close();
                        } catch (IOException ignored) {}
                    }
                }
            }
        });
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }

    private String join(String[] strings) {
        if (strings == null) {
            return null;
        }
        return String.join(",\n\t\t", strings);
    }
    
    private String dumpSocketDetails(Socket socket) {
        if (socket instanceof SSLSocket) {
            SSLSocket sslSocket = (SSLSocket)socket;
            return String.format("SSLSocket[hostname=%s, port=%d, Session(cipher=%s, protocol=%s)", 
                    sslSocket.getInetAddress().getHostAddress(),
                    sslSocket.getLocalPort(),
                    sslSocket.getSession().getCipherSuite(),
                    sslSocket.getSession().getProtocol());
        }
        else {
            return socket.toString();
        }
    }
    public void start(TlsPolicy policy) throws IOException {
        ServerSocket socketServer;
        if (policy != null) {
            if (policy.context == null) {
                throw new AerospikeException("Remote Server has a TLS Policy specified but it has no SSL context on it.");
            }
            SSLServerSocket sslSocket = (SSLServerSocket)policy.context.getServerSocketFactory().createServerSocket(port);
            sslSocket.setUseClientMode(false);
            if (policy.ciphers != null) {
                sslSocket.setEnabledCipherSuites(policy.ciphers);
            }
            if (policy.protocols != null) {
                sslSocket.setEnabledProtocols(policy.protocols);
            }
            socketServer = sslSocket;
            System.out.printf("Starting remote server with TLS configuration: %s\n", socketServer);
            if (verbose) {
                SSLParameters params = sslSocket.getSSLParameters();
                System.out.printf("\tEndpoint identification algorithm: %s\n", params.getEndpointIdentificationAlgorithm());
                System.out.printf("\tApplication Protocols: %s\n", join(params.getApplicationProtocols()));
                System.out.printf("\tCipher Suites: %s\n", join(params.getCipherSuites()));
                System.out.printf("\tProtocols: %s\n", join(params.getProtocols()));
                System.out.printf("\tNeeds Client Auth: %s\n", params.getNeedClientAuth());
            }
        }
        else {
            socketServer = new ServerSocket(port);
        }
        System.out.printf("Comparator remote server listening on port %d\n", port);
        while (true) {
            Socket socket = null;
            try {
                socket = socketServer.accept();
                System.out.printf("New client connection established: %s\n", dumpSocketDetails(socket));
                
                SocketHandler handler = new SocketHandler(socket, client, this.debug);
                Thread thread = new Thread(handler);
                thread.start();
            }
            catch (IOException ioe) {
                // Ignore exceptions in establishing communications, this can be caused by heartbeat protocols.
//                System.err.println("Error attaching to socket. Ignoring and continuing.");
//                RemoteUtils.handleIOException(ioe);
                
            }
        }
//        socketServer.close();
    }
    
    private static class SocketHandler implements Runnable {
        private final AerospikeClientAccess client;
        private final DataInputStream dis;
        private final DataOutputStream dos;
        private final Socket socket;
        private final boolean debug;
        
        public SocketHandler(Socket socket, AerospikeClientAccess client, boolean debug) throws IOException {
            this.client = client;
            this.socket = socket;
            this.debug = debug;
            this.dis = new DataInputStream(socket.getInputStream()); 
            this.dos = new DataOutputStream(socket.getOutputStream());
        }
        
        private void doQueryPartition() throws IOException {
            QueryPolicy qp = new QueryPolicy();
            qp.maxConcurrentNodes = dis.readInt();
            qp.includeBinData = dis.readBoolean();
            qp.shortQuery = dis.readBoolean();
            int length = dis.readInt();
            if (length == 0) {
                qp.filterExp = null;
            }
            else {
                byte[] bytes = dis.readNBytes(length);
                qp.filterExp = Expression.fromBytes(bytes);
            }
            
            Statement stmt = new Statement();
            stmt.setNamespace(dis.readUTF());
            stmt.setSetName(dis.readUTF());
            stmt.setRecordsPerSecond(dis.readInt());
            
            int begin = dis.readInt();
            int count = dis.readInt();
            PartitionFilter partitionFilter = PartitionFilter.range(begin, count);
            RecordSetAccess recordsSet = client.queryPartitions(qp, stmt, partitionFilter);
            dos.writeUTF("Ready");
            boolean done = false;
            while (!done) {
                int command = dis.read();
                switch (command) {
                case CMD_RS_MULTI:
                    int num = dis.readInt();
                    long now = 0;
                    int recordsReturned = 0;
                    if (this.debug) {
                        System.out.printf("Processing request for %s records\n", num);
                        now = System.nanoTime();
                    }
                    boolean hasMore = recordsSet.next();
                    for (int i = 0; hasMore && i < num; i++) {
                        dos.writeBoolean(true);
                        RemoteUtils.sendKey(recordsSet.getKey(), dos);
                        RemoteUtils.sendRecord(recordsSet.getRecord(), dos);
                        hasMore = recordsSet.next();
                        recordsReturned++;
                    }
                    if (!hasMore) {
                        dos.writeBoolean(false);
                    }
                    if (this.debug) {
                        long time = System.nanoTime() - now;
                        System.out.printf("Finished processing request for %,d records in %,dus (%,d returned)\n", num, time/1000, recordsReturned);
                    }
                    break;
                    
                case CMD_RS_MULTI_RECORD_HASH:
                    num = dis.readInt();
                    now = 0;
                    recordsReturned = 0;
                    if (this.debug) {
                        System.out.printf("Processing request for %s records\n", num);
                        now = System.nanoTime();
                    }
                    hasMore = recordsSet.next();
                    for (int i = 0; hasMore && i < num; i++) {
                        dos.writeBoolean(true);
                        RemoteUtils.sendKey(recordsSet.getKey(), dos);
                        RemoteUtils.sendRecordHash(recordsSet.getRecord(), dos);
                        hasMore = recordsSet.next();
                        recordsReturned++;
                    }
                    if (!hasMore) {
                        dos.writeBoolean(false);
                    }
                    if (this.debug) {
                        long time = System.nanoTime() - now;
                        System.out.printf("Finished processing request for %,d records in %,dus (%,d returned)\n", num, time/1000, recordsReturned);
                    }
                    break;
                case CMD_RS_NEXT:
                    now = 0;
                    if (this.debug) {
                        System.out.printf("Processing request for next record\n");
                        now = System.nanoTime();
                    }
                    dos.writeBoolean(recordsSet.next());
                    if (this.debug) {
                        long time = System.nanoTime() - now;
                        System.out.printf("Finished processing request for next records in %,dus\n", time/1000);
                    }
                    break;
                    
                case CMD_RS_KEY:
                    now = 0;
                    if (this.debug) {
                        System.out.printf("Processing for key\n");
                        now = System.nanoTime();
                    }
                    Key key = recordsSet.getKey();
                    RemoteUtils.sendKey(key, dos);
                    if (this.debug) {
                        long time = System.nanoTime() - now;
                        System.out.printf("Finished processing request for key in %,dus\n", time/1000);
                    }
                    break;
                 
                case CMD_RS_RECORD:
                    now = 0;
                    if (this.debug) {
                        System.out.printf("Processing request for record\n");
                        now = System.nanoTime();
                    }
                    Record record = recordsSet.getRecord();
                    RemoteUtils.sendRecord(record, dos);
                    if (this.debug) {
                        long time = System.nanoTime() - now;
                        System.out.printf("Finished processing request for record in %,dus\n", time/1000);
                    }
                    break;
                    
                case CMD_RS_RECORD_HASH:
                    now = 0;
                    if (this.debug) {
                        System.out.printf("Processing request for record\n");
                        now = System.nanoTime();
                    }
                    record = recordsSet.getRecord();
                    RemoteUtils.sendRecordHash(record, dos);
                    if (this.debug) {
                        long time = System.nanoTime() - now;
                        System.out.printf("Finished processing request for record in %,dus\n", time/1000);
                    }
                    break;
                    
                case CMD_RS_CLOSE:
                    recordsSet.close();
                    done = true;
                    dos.writeInt(0);
                }
            }
        }
        
        private void doTouch() throws IOException {
            WritePolicy policy = new WritePolicy();
            policy = (WritePolicy) RemoteUtils.readPolicy(policy, dis);
            Key key = RemoteUtils.readKey(dis);
            client.touch(policy, key);
            dos.writeInt(0);
        }
        
        private void doGet() throws IOException {
            WritePolicy policy = new WritePolicy();
            policy = (WritePolicy) RemoteUtils.readPolicy(policy, dis);
            Key key = RemoteUtils.readKey(dis);
            Record record = client.get(policy, key);
            RemoteUtils.sendRecord(record, dos);
        }
        
        private void doInvokeInfoCmdOnAllNodes() throws IOException {
            String command = dis.readUTF();
            Map<String, String> results = client.invokeInfoCommandOnAllNodes(command);
            dos.writeInt(results.size());
            for (String key : results.keySet()) {
                dos.writeUTF(key);
                dos.writeUTF(results.get(key));
            }
        }
        
        private void doInvokeInfoCommandOnANode() throws IOException {
            String command = dis.readUTF();
            dos.writeUTF(client.invokeInfoCommandOnANode(command));
        }
        
        private void doGetNodeNames() throws IOException {
            List<String> nodeNames = client.getNodeNames();
            dos.writeInt(nodeNames.size());
            for (String thisNodeName : nodeNames) {
                dos.writeUTF(thisNodeName);
            }
        }
        
        @Override
        public void run() {
            boolean done = false;
            while (!done) {
                try {
                    int command = dis.read();
                    switch (command) {
                    case CMD_CLOSE:
                        done = true;
                        dos.writeInt(0);
                        break;
                        
                    case CMD_TOUCH:
                        doTouch();
                        break;
                        
                    case CMD_GET:
                        doGet();
                        break;
                        
                    case CMD_QUERY_PARTITION:
                        doQueryPartition();
                        break;
                        
                    case CMD_INVOKE_INFO_CMD_ON_ALL_NODES:
                        doInvokeInfoCmdOnAllNodes();
                        break;
                        
                    case CMD_INVOKE_INFO_CMD_ON_A_NODE:
                        doInvokeInfoCommandOnANode();
                        break;
                    case CMD_GET_NODE_NAMES:
                        doGetNodeNames();
                    }
                }
                catch (IOException ioe) {
                    System.out.println("IOException received, closing");
                    RemoteUtils.handleIOException(ioe);
                    break;
                }
            }
            try {
                dis.close();
                dos.close();
                socket.close();
            }
            catch (IOException ignored) {}
            
        }
        
    }
}

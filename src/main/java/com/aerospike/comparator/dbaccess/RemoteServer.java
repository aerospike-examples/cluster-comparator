package com.aerospike.comparator.dbaccess;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;

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
    
    private final AerospikeClientAccess client;
    private final int port;
    
    public RemoteServer(AerospikeClientAccess client, int port) {
        this.client = client;
        this.port = port;
    }
    
    public void start(TlsPolicy policy) throws IOException {
        ServerSocket socketServer;
        if (policy != null) {
            if (policy.context == null) {
                throw new AerospikeException("Remote Server has a TLS Policy specified but it has no SSL context on it.");
            }
            socketServer = policy.context.getServerSocketFactory().createServerSocket(port);
            System.out.printf("Starting remote server with TLS configuration: %s\n", socketServer);
        }
        else {
            socketServer = new ServerSocket(port);
        }
        System.out.printf("Comparator remote server listening on port %d\n", port);
        while (true) {
            Socket socket = null;
            try {
                socket = socketServer.accept();
                System.out.printf("New client connection established: %s\n", socket);
                
                SocketHandler handler = new SocketHandler(socket, client);
                Thread thread = new Thread(handler);
                thread.start();
            }
            catch (IOException ioe) {
                ioe.printStackTrace();
                break;
            }
        }
        socketServer.close();
    }
    
    private static class SocketHandler implements Runnable {
        private final AerospikeClientAccess client;
        private final DataInputStream dis;
        private final DataOutputStream dos;
        private final Socket socket;
        
        public SocketHandler(Socket socket, AerospikeClientAccess client) throws IOException {
            this.client = client;
            this.socket = socket;
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
                case CMD_RS_NEXT:
                    dos.writeBoolean(recordsSet.next());
                    break;
                    
                case CMD_RS_KEY:
                    Key key = recordsSet.getKey();
                    dos.writeUTF(key.namespace);
                    dos.writeUTF(key.setName);
                    dos.writeInt(key.digest.length);
                    dos.write(key.digest);
                    break;
                 
                case CMD_RS_RECORD:
                    Record record = recordsSet.getRecord();
                    RemoteUtils.sendRecord(record, dos);
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

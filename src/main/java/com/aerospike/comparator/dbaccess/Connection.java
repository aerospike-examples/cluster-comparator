package com.aerospike.comparator.dbaccess;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

class Connection {
    private final Socket socket;
    private final DataInputStream dis;
    private final DataOutputStream dos;
    
    public Connection(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        this.dis = new DataInputStream(socket.getInputStream());
        this.dos = new DataOutputStream(socket.getOutputStream());
    }
    
    public void close() {
        try {
            this.dis.close();
            this.dos.close();
            this.socket.close();
        }
        catch (IOException ignored) {}
    }
    
    public DataInputStream getDis() {
        return dis;
    }
    public DataOutputStream getDos() {
        return dos;
    }
}
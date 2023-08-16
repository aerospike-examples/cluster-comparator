package com.aerospike.comparator.dbaccess;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.AerospikeException.InvalidNode;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.ClusterUtilities;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.Replica;

import gnu.crypto.hash.RipeMD160;

public class RemoteUtils {
    public static void sendKey(Key key, DataOutputStream dos) throws IOException {
        dos.writeUTF(key.namespace);
        dos.writeUTF(key.setName);
        dos.writeInt(key.digest.length);
        dos.write(key.digest);
        if (key.userKey != null) {
            int type = key.userKey.getType();
            dos.writeInt(type);
            Object value = key.userKey.getObject();
            switch (type) {
            case ParticleType.BLOB:
                byte[] bytes = (byte[]) value;
                dos.writeInt(bytes.length);
                dos.write(bytes);
                break;
            case ParticleType.DOUBLE:
                Double doubleNumber = (Double)value;
                dos.writeDouble(doubleNumber);
                break;
            case ParticleType.INTEGER:
                dos.writeLong(key.userKey.toLong());
                break;
            case ParticleType.STRING:
                dos.writeUTF(key.userKey.toString());
                break;
            }
        }
        else {
            dos.writeInt(0);
        }
    }
    
    public static Key readKey(DataInputStream dis) throws IOException {
        String namespace = dis.readUTF();
        String setName = dis.readUTF();
        int length = dis.readInt();
        byte[] bytes = dis.readNBytes(length);
        int type = dis.readInt();
        Value value = null;
        if (type == ParticleType.BLOB) {
            length = dis.readInt();
            byte[] keyBytes = dis.readNBytes(length);
            value = Value.get(keyBytes);
        }
        else if (type == ParticleType.DOUBLE) {
            value = Value.get(dis.readDouble());
        }
        else if (type == ParticleType.INTEGER) {
            value = Value.get(dis.readLong());
        }
        else if (type == ParticleType.STRING) {
            value = Value.get(dis.readUTF());
        }
        return new Key(namespace, bytes, setName, value);
    }
    
    public static void sendPolicy(Policy policy, DataOutputStream dos) throws IOException {
        if (policy == null) {
            dos.writeBoolean(false);
        }
        else {
            dos.writeBoolean(true);
            dos.writeInt(policy.connectTimeout);
            dos.writeInt(policy.maxRetries);
            dos.writeInt(policy.sleepBetweenRetries);
            dos.writeInt(policy.socketTimeout);
            dos.writeInt(policy.totalTimeout);
            dos.writeBoolean(policy.sendKey);
            dos.writeUTF(policy.replica.toString());
        }
    }
    
    public static Policy readPolicy(Policy policyToChange, DataInputStream dis) throws IOException {
        if (dis.readBoolean() == false) {
            return null;
        }
        else {
            policyToChange.connectTimeout = dis.readInt();
            policyToChange.maxRetries = dis.readInt();
            policyToChange.sleepBetweenRetries = dis.readInt();
            policyToChange.socketTimeout = dis.readInt();
            policyToChange.totalTimeout = dis.readInt();
            policyToChange.sendKey = dis.readBoolean();
            policyToChange.replica = Replica.valueOf(dis.readUTF());
            return policyToChange;
        }
    }

    public static void sendRecord(Record record, DataOutputStream dos) throws IOException{
        if (record == null) {
            dos.writeBoolean(false);
        }
        else {
            dos.writeBoolean(true);
            dos.writeInt(record.expiration);
            dos.writeInt(record.generation);
            Map<String, Object> map = record.bins;
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                oos.writeObject(map);
                bos.flush();
                byte[] bytes = bos.toByteArray();
                dos.writeInt(bytes.length);
                dos.write(bytes);
            }
        }
    }
    
    public static Record readRecord(DataInputStream dis) throws IOException {
        boolean exists = dis.readBoolean();
        if (exists) {
            int expiration = dis.readInt();
            int generation = dis.readInt();
            int length = dis.readInt();
            byte[] bytes = dis.readNBytes(length);
            try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                    ObjectInputStream ois = new ObjectInputStream(bis)) {
                Map<String, Object> map = (Map<String, Object>) ois.readObject();
                return new Record(map, generation, expiration);
            }
            catch (ClassNotFoundException cnfe) {
                throw new AerospikeException(cnfe);
            }
        }
        else {
            return null;
        }
    }
    
    public static void sendRecordHash(Record record, DataOutputStream dos) throws IOException{
        if (record == null) {
            dos.writeBoolean(false);
        }
        else {
            dos.writeBoolean(true);
            dos.writeInt(record.expiration);
            dos.writeInt(record.generation);
            byte[] hash = getRecordHash(record);
            dos.writeInt(hash.length);
            dos.write(hash);
        }
    }
    
    public static byte[] readRecordHash(DataInputStream dis) throws IOException {
        boolean exists = dis.readBoolean();
        if (exists) {
            /*int expiration = */ dis.readInt();
            /*int generation = */ dis.readInt();
            int length = dis.readInt();
            byte[] bytes = dis.readNBytes(length);
            return bytes;
        }
        else {
            return null;
        }
    }
    
    public static void handleIOException(IOException ioe) {
        ioe.printStackTrace();
    }

    public static void handleInvalidNode(InvalidNode in, IAerospikeClient client) {
        ClusterUtilities clusterUtils = new ClusterUtilities(client);
        clusterUtils.printInfo(true);
    }
    
    public static byte[] getRecordHash(Record record) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(record.bins);
            bos.flush();
            byte[] bytes = bos.toByteArray();
            
            RipeMD160 hash = new RipeMD160();
            hash.update(bytes, 0, bytes.length);
            return hash.digest();
        }
        catch (IOException ioe) {
            throw new AerospikeException(ioe);
        }
    }
    
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.printf("Usage: RemoteUtils <address> <port>. Sends a quick ping to the node's port over TCP/IP\n");
            System.exit(-1);
        }
        Socket socket = new Socket(args[0], Integer.parseInt(args[1]));
        socket.close();
        System.out.printf("Ping successful to %s:%s\n", args[0], args[1]);
    }
}

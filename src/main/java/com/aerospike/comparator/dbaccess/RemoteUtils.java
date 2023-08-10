package com.aerospike.comparator.dbaccess;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.Replica;

public class RemoteUtils {
    public static void sendKey(Key key, DataOutputStream dos) throws IOException {
        dos.writeUTF(key.namespace);
        dos.writeUTF(key.setName);
        dos.writeInt(key.digest.length);
        dos.write(key.digest);
    }
    
    public static Key readKey(DataInputStream dis) throws IOException {
        String namespace = dis.readUTF();
        String setName = dis.readUTF();
        int length = dis.readInt();
        byte[] bytes = dis.readNBytes(length);
        return new Key(namespace, bytes, setName, null);
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
    
    public static void handleIOException(IOException ioe) {
        ioe.printStackTrace();
    }
}

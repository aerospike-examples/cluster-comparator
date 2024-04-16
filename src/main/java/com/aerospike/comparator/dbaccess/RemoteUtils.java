package com.aerospike.comparator.dbaccess;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import com.aerospike.comparator.AerospikeComparator;

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
    
    public static void sendRecordMetadata(RecordMetadata record, DataOutputStream dos) throws IOException{
        if (record == null) {
            dos.writeBoolean(false);
        }
        else {
            dos.writeBoolean(true);
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                oos.writeObject(record);
                bos.flush();
                byte[] bytes = bos.toByteArray();
                dos.writeInt(bytes.length);
                dos.write(bytes);
            }
        }
    }
    
    public static RecordMetadata readRecordMetadata(DataInputStream dis) throws IOException {
        boolean exists = dis.readBoolean();
        if (exists) {
            int length = dis.readInt();
            byte[] bytes = dis.readNBytes(length);
            try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                    ObjectInputStream ois = new ObjectInputStream(bis)) {
                RecordMetadata result = (RecordMetadata) ois.readObject();
                return result;
            }
            catch (ClassNotFoundException cnfe) {
                throw new AerospikeException(cnfe);
            }
        }
        else {
            return null;
        }
    }
    
    public static void sendRecordHash(Record record, DataOutputStream dos, boolean sortMaps) throws IOException{
        if (record == null) {
            dos.writeBoolean(false);
        }
        else {
            dos.writeBoolean(true);
            dos.writeInt(record.expiration);
            dos.writeInt(record.generation);
            byte[] hash = getRecordHash(record, sortMaps);
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
    
    private static byte[] getHash(Object object) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(object);
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
    
    private static List<Object> sortToAerospikeOrder(Collection<Object> objects, AerospikeComparator comparator) {
        List<Object> result = new ArrayList<>(objects);
        Collections.sort(result, comparator);
        return result;
    }
    
    private static Object turnAnyMapsToLists(Object object, AerospikeComparator comparator) {
        if (object instanceof Map) {
            return turnMapToKeySortedList((Map<?, ?>) object, comparator);
        }
        else if (object instanceof List) {
            List<Object> list = (List<Object>)object;
            for (int i = 0; i < list.size(); i++) {
                Object obj = list.get(i);
                Object newObj = turnAnyMapsToLists(obj, comparator);
                // deliberately compare object references
                if (obj != newObj) {
                    list.set(i, newObj);
                }
            }
        }
        return object;
    }
    
    private static List<Entry<?, ?>> turnMapToKeySortedList(Map<?, ?> map, AerospikeComparator comparator) {
        List<?> sortedKeys = new ArrayList<Object>(map.keySet());
        Collections.sort(sortedKeys, comparator);
        List<Entry<?, ?>> mapAsList = new ArrayList<>();
        for (Object key : sortedKeys ) {
            Object newKey = turnAnyMapsToLists(key, comparator);
            Object newValue = turnAnyMapsToLists(map.get(key), comparator);
            mapAsList.add(new SimpleEntry<>(newKey, newValue));
        }
        return mapAsList; 
    }
    
    private static byte[] getMapHash(Map<?, ?> map) {
        List<Entry<?, ?>> list = turnMapToKeySortedList(map, new AerospikeComparator());
        return getHash(list);
    }
    
    /**
     * Return a hash representing the record. This has is 20 bytes of random noise computed by <code>RIPEMD160</code>
     * hashing algorithm. Whilst this is not guaranteed to be unique, the probability of 2 hashes colliding is ~1 in 1e48
     * so is effectively zero.
     * <p/>
     * Note that we cannot just blindly hash the record. Maps (including the record bins) should be order independent,
     * resulting in extra work.
     * @param record
     * @return The hash of the passed record.
     */
    public static byte[] getRecordHash(Record record, boolean sortMaps) {
        if (sortMaps) {
            return getMapHash(record.bins);
        }
        else {
            return getHash(record.bins);
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

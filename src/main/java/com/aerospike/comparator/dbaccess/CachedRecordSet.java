package com.aerospike.comparator.dbaccess;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.comparator.ClusterComparatorOptions.CompareMode;

public class CachedRecordSet {
    private static class Entry {
        boolean hasNext;
        Record record;
        byte[] recordHash;
        Key key;
        
        public Entry(boolean hasNext, Key key) {
            this.hasNext = hasNext;
            this.record = null;
            this.key = key;
        }
        public Entry(boolean hasNext, Key key, Record record) {
            this.hasNext = hasNext;
            this.record = record;
            this.key = key;
        }
        public Entry(boolean hasNext, Key key, byte[] recordHash) {
            this.hasNext = hasNext;
            this.recordHash = recordHash;
            this.key = key;
        }
        public Record getRecord() {
            return record;
        }
        public Key getKey() {
            return key;
        }
        public boolean hasNext() {
            return this.hasNext;
        }
        public byte[] getRecordHash() {
            return recordHash;
        }
    }
    
    private final Entry END_OF_STREAM_ENTRY = new Entry(false, null, (Record)null);
    private final int cacheSize;
    private final ArrayBlockingQueue<Entry> queue;
    private final Thread fillingThread;
    private Entry current;
    private volatile boolean close = false;
    private volatile boolean isFilling = false;
    private volatile boolean isFinished = false;
    private volatile boolean threadTerminated = false;
    private final Object lock = new Object();
    private final Connection connection;
    private final boolean storeHashes;
    private final CompareMode compareMode;
    
    public CachedRecordSet(final int cacheSize, final Connection connection, final boolean storeHashes, final CompareMode compareMode) {
        this.cacheSize = cacheSize;
        this.connection = connection;
        this.queue = new ArrayBlockingQueue<>(cacheSize);
        this.storeHashes = storeHashes;
        this.compareMode = compareMode;
        this.fillingThread = new Thread(() -> {
            try {
                this.isFinished = !readMulti(cacheSize, connection);
                while ((!close) && (!isFinished)) {
                    synchronized(lock) {
                        // There's a tiny window where we could miss the notification 
                        // and worst case enter a deadlock situation. So wait 50ms then wake up
                        // and check the queue again.
                        lock.wait(50);
                    }
                    if (close) {
                        break;
                    }
                    else {
                        this.isFinished = !readMulti(queue.remainingCapacity(), connection);
                    }
                }
                connection.getDos().write(RemoteServer.CMD_RS_CLOSE);
                connection.getDis().readInt();
            }
            catch (IOException ioe) {
                RemoteUtils.handleIOException(ioe);
            }
            catch (InterruptedException ie) {
                // TODO: Should we just ignore this?
                // ie.printStackTrace();
            }
            finally {
                this.threadTerminated = true;
            }
        });
        this.fillingThread.setDaemon(true);
        this.fillingThread.setName("cache-filling-thread");
        this.fillingThread.start();
    }
    
    private boolean readMulti(int size, Connection connection) throws IOException, InterruptedException {
        this.isFilling = true;
        int command;
        if (CompareMode.MISSING_RECORDS == this.compareMode) {
            command = RemoteServer.CMD_RS_MULTI_KEY_ONLY;
        }
        else if (storeHashes) {
            command = RemoteServer.CMD_RS_MULTI_RECORD_HASH;
        }
        else  {
            command = RemoteServer.CMD_RS_MULTI;
        }
        connection.getDos().write(command);
        connection.getDos().writeInt(size);
        boolean hasMore = true;
        for (int i = 0; i < size && !close; i++) {
            boolean hasNext = connection.getDis().readBoolean();
            if (!hasNext) {
                this.queue.put(END_OF_STREAM_ENTRY);
                hasMore = false;
                break;
            }
            else {
                Key key = RemoteUtils.readKey(connection.getDis());
                if (command == RemoteServer.CMD_RS_MULTI_KEY_ONLY) {
                    // Missing records mode requires only keys
                    this.queue.put(new Entry(
                            true, key));
                }
                else if (command == RemoteServer.CMD_RS_MULTI_RECORD_HASH) {
                    this.queue.put(new Entry(
                            true, key,
                            RemoteUtils.readRecordHash(connection.getDis())));
                }
                else {
                    this.queue.put(new Entry(
                            true, key,
                            RemoteUtils.readRecord(connection.getDis())));
                }
            }
        }
        isFilling = false;
        return hasMore;
    }
    public boolean next() {
        if (close || (current != null && !current.hasNext())) {
            return false;
        }
        try {
            if (!isFinished && !isFilling && this.queue.remainingCapacity() > cacheSize/2) {
                synchronized(lock) {
                    lock.notify();
                }
            }
            this.current = this.queue.take();
        } catch (InterruptedException e) {
            throw new AerospikeException(e);
        }
        return this.current.hasNext();
    }
    
    public byte[] getRecordHash() {
        if (this.storeHashes) {
            if (this.current != null) {
                return this.current.getRecordHash();
            }
            return null;
        }
        else {
            throw new IllegalAccessError("getRecordHash cannot be called if storeHashes is false");
        }
    }
    public Record getRecord() {
        if (!this.storeHashes) {
            if (this.current != null) {
                return this.current.getRecord();
            }
            return null;
        }
        else {
            throw new IllegalAccessError("getRecord cannot be called if storeHashes is true");
        }
    }
    
    public Key getKey() {
        if (this.current != null) {
            return this.current.getKey();
        }
        return null;
    }

    public void close() throws IOException {
        this.close = true;
        this.fillingThread.interrupt();
        while (!this.threadTerminated) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}

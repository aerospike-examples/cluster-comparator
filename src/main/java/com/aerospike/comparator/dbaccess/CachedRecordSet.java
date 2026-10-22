package com.aerospike.comparator.dbaccess;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

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
    /** Set only after CMD_RS_CLOSE completed. Anything else must not return the socket to the pool. */
    private volatile boolean cleanShutdown = false;
    private volatile IOException fillError;
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
                // The in-flight batch has been fully read, so the server is waiting for the next
                // command. Closing here keeps the socket aligned for the next user of the pool.
                connection.getDos().write(RemoteServer.CMD_RS_CLOSE);
                connection.getDis().readInt();
                this.cleanShutdown = true;
            }
            catch (IOException ioe) {
                // next() throws this. The socket is left out of the pool by RemoteRecordSet.close().
                this.fillError = ioe;
            }
            catch (InterruptedException ie) {
                this.fillError = new IOException("Remote cache fill was interrupted", ie);
                Thread.currentThread().interrupt();
            }
            finally {
                // Unblock a caller waiting in next() when the fill stops early.
                this.queue.offer(END_OF_STREAM_ENTRY);
                this.threadTerminated = true;
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        });
        this.fillingThread.setDaemon(true);
        this.fillingThread.setName("cache-filling-thread");
        this.fillingThread.start();
    }
    
    private boolean readMulti(int size, Connection connection) throws IOException, InterruptedException {
        this.isFilling = true;
        try {
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
            // Finish this batch even if close() has been called. Stopping mid-response would leave
            // unread bytes on the socket, and the connection could not be used again.
            for (int i = 0; i < size; i++) {
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
            return hasMore;
        }
        finally {
            isFilling = false;
        }
    }
    public boolean next() {
        if (close || (current != null && !current.hasNext())) {
            return false;
        }
        try {
            while (true) {
                if (!isFinished && !isFilling && this.queue.remainingCapacity() > cacheSize/2) {
                    synchronized(lock) {
                        lock.notify();
                    }
                }
                Entry entry = this.queue.poll(50, TimeUnit.MILLISECONDS);
                if (entry != null) {
                    this.current = entry;
                    if (!entry.hasNext()) {
                        if (fillError != null) {
                            throw new AerospikeException(fillError);
                        }
                        return false;
                    }
                    return true;
                }
                if (threadTerminated) {
                    if (fillError != null) {
                        throw new AerospikeException(fillError);
                    }
                    return false;
                }
            }
        } catch (InterruptedException e) {
            throw new AerospikeException(e);
        }
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

    /**
     * @return true when the server acknowledged {@code CMD_RS_CLOSE} and the socket can be reused
     */
    public boolean close() {
        this.close = true;
        synchronized (lock) {
            lock.notifyAll();
        }
        while (!this.threadTerminated) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return this.cleanShutdown;
    }
}

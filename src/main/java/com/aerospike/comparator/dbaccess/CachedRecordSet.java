package com.aerospike.comparator.dbaccess;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

public class CachedRecordSet {
    private static class Entry {
        boolean hasNext;
        Record record;
        Key key;
        
        public Entry(boolean hasNext, Key key, Record record) {
            this.hasNext = hasNext;
            this.record = record;
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
    }
    
    private final int cacheSize;
    private final ArrayBlockingQueue<Entry> queue;
    private final Thread fillingThread;
    private Entry current;
    private volatile boolean close = false;
    private volatile boolean isFilling = false;
    private volatile boolean isFinished = false;
    private final Object lock = new Object();
    private final Connection connection;
    
    public CachedRecordSet(int cacheSize, Connection connection) {
        this.cacheSize = cacheSize;
        this.connection = connection;
        this.queue = new ArrayBlockingQueue<>(cacheSize);
        this.fillingThread = new Thread(() -> {
            try {
                this.isFinished = !readMulti(cacheSize, connection);
                while ((!close) && (!isFinished)) {
                    synchronized(lock) {
                        // There's a tiny window where we could miss the notification 
                        // and worst case enter a deadlock situation. So wait 50ms then wake up
                        // and check the queu again.
                        lock.wait(50);
                    }
                    if (close) {
                        break;
                    }
                    else {
                        this.isFinished = !readMulti(queue.remainingCapacity(), connection);
                    }
                }
            }
            catch (IOException ioe) {
                RemoteUtils.handleIOException(ioe);
            }
            catch (InterruptedException ie) {
                // TODO: Should we just ignore this?
                // ie.printStackTrace();
            }
        });
        this.fillingThread.setDaemon(true);
        this.fillingThread.start();
    }
    
    private boolean readMulti(int size, Connection connection) throws IOException, InterruptedException {
        isFilling = true;
        connection.getDos().write(RemoteServer.CMD_RS_MULTI);
        connection.getDos().writeInt(size);
        boolean hasMore = true;
        for (int i = 0; i < size && !close; i++) {
            boolean hasNext = connection.getDis().readBoolean();
            if (!hasNext) {
                this.queue.put(new Entry(false, null, null));
                hasMore = false;
                break;
            }
            else {
                this.queue.put(new Entry(
                        true,
                        RemoteUtils.readKey(connection.getDis()),
                        RemoteUtils.readRecord(connection.getDis())));
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
    public Record getRecord() {
        if (this.current != null) {
            return this.current.getRecord();
        }
        return null;
    }
    
    public Key getKey() {
        if (this.current != null) {
            return this.current.getKey();
        }
        return null;
    }

    public void close() throws IOException {
        connection.getDos().write(RemoteServer.CMD_RS_CLOSE);
        connection.getDis().readInt();
        this.close = true;
        this.fillingThread.interrupt();
    }
}

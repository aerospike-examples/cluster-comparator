package com.aerospike.comparator.dbaccess;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.Statement;
import com.aerospike.comparator.ClusterComparatorOptions;
import com.aerospike.comparator.ClusterComparatorOptions.CompareMode;

/**
 * Remote link behaviour that does not need a live Aerospike cluster: a local socket speaks the comparator protocol.
 */
public class RemoteProtocolTest {

    @Test
    void deleteSendsDeleteCommand() throws Exception {
        AtomicInteger command = new AtomicInteger(-1);
        try (FakeRemote remote = new FakeRemote(socket -> {
            DataInputStream in = input(socket);
            DataOutputStream out = output(socket);
            expectConfig(in, out);
            int cmd = in.read();
            command.set(cmd);
            RemoteUtils.readPolicy(new WritePolicy(), in);
            RemoteUtils.readKey(in);
            out.writeInt(0);
            out.flush();
            expectClose(in, out);
        })) {
            RemoteAerospikeClient client = client(remote);
            client.delete(new WritePolicy(), new Key("test", "users", "user-1"));
            client.close();
        }
        assertEquals(RemoteServer.CMD_DELETE, command.get());
    }

    @Test
    void nullSetNameRoundTripsAsNull() throws Exception {
        Key template = new Key("test", "users", "user-1");
        Key keylessNullSet = new Key("test", template.digest, null, null);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        RemoteUtils.sendKey(keylessNullSet, out);
        out.flush();

        Key read = RemoteUtils.readKey(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        assertNull(read.setName);
        assertNull(read.userKey);
        assertArrayEquals(template.digest, read.digest);

        Key named = new Key("test", "users", "user-1");
        bytes.reset();
        out = new DataOutputStream(bytes);
        RemoteUtils.sendKey(named, out);
        out.flush();
        Key namedRead = RemoteUtils.readKey(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        assertEquals("users", namedRead.setName);
        assertEquals("user-1", namedRead.userKey.getObject());
    }

    @Test
    void queryPartitionsEncodesNullSetNameAsEmptyString() throws Exception {
        AtomicReference<String> wireSetName = new AtomicReference<>();
        try (FakeRemote remote = new FakeRemote(socket -> {
            DataInputStream in = input(socket);
            DataOutputStream out = output(socket);
            expectConfig(in, out);
            assertEquals(RemoteServer.CMD_QUERY_PARTITION, in.read());
            in.readInt();
            in.readBoolean();
            in.readBoolean();
            int expLength = in.readInt();
            if (expLength > 0) {
                in.readNBytes(expLength);
            }
            in.readUTF();
            wireSetName.set(in.readUTF());
            in.readInt();
            in.readInt();
            in.readInt();
            out.writeUTF("Ready");
            out.flush();
            assertEquals(RemoteServer.CMD_RS_CLOSE, in.read());
            out.writeInt(0);
            out.flush();
            expectClose(in, out);
        })) {
            RemoteAerospikeClient client = client(remote);
            Statement statement = new Statement();
            statement.setNamespace("test");
            statement.setSetName(null);
            RecordSetAccess records = client.queryPartitions(new QueryPolicy(), statement, PartitionFilter.id(0));
            records.close();
            client.close();
        }
        assertEquals("", wireSetName.get());
        assertNull(RemoteUtils.readNullableString(utf("")));
    }

    @Test
    void failedCommandDiscardsConnectionSoTheNextCallOpensANewOne() throws Exception {
        AtomicInteger accepts = new AtomicInteger();
        AtomicInteger deletes = new AtomicInteger();
        try (FakeRemote remote = new FakeRemote(socket -> {
            accepts.incrementAndGet();
            DataInputStream in = input(socket);
            DataOutputStream out = output(socket);
            while (true) {
                int cmd = in.read();
                if (cmd == -1) {
                    return;
                }
                if (cmd == RemoteServer.CMD_CONFIG) {
                    in.readBoolean();
                    out.writeInt(0);
                    out.flush();
                }
                else if (cmd == RemoteServer.CMD_DELETE) {
                    if (deletes.incrementAndGet() == 1) {
                        socket.close();
                        return;
                    }
                    RemoteUtils.readPolicy(new WritePolicy(), in);
                    RemoteUtils.readKey(in);
                    out.writeInt(0);
                    out.flush();
                }
                else if (cmd == RemoteServer.CMD_CLOSE) {
                    out.writeInt(0);
                    out.flush();
                    return;
                }
                else {
                    throw new IOException("Unexpected command " + cmd);
                }
            }
        })) {
            RemoteAerospikeClient client = client(remote);
            Key key = new Key("test", "users", "user-1");
            assertThrows(AerospikeException.class, () -> client.delete(new WritePolicy(), key));
            client.delete(new WritePolicy(), key);
            client.close();
        }
        assertEquals(2, deletes.get());
        assertTrue(accepts.get() >= 2, "expected a new socket after the failed delete, accepts=" + accepts.get());
    }

    @Test
    void discardDoesNotReturnTheClosedSocketToThePool() throws Exception {
        try (FakeRemote remote = new FakeRemote(socket -> {
            try {
                DataInputStream in = input(socket);
                DataOutputStream out = output(socket);
                int marker = in.read();
                if (marker >= 0) {
                    out.write(marker);
                    out.flush();
                }
            }
            catch (IOException ignored) {
                // The discarded connection is closed while this read is still waiting.
            }
        })) {
            ConnectionPool pool = new ConnectionPool("127.0.0.1", remote.port(), 2, null);
            Connection first = pool.borrow();
            pool.discard(first);
            Connection second = pool.borrow();
            try {
                assertNotSame(first, second);
                assertThrows(IOException.class, () -> {
                    first.getDos().write(7);
                    first.getDos().flush();
                });
                second.getDos().write(7);
                second.getDos().flush();
                assertEquals(7, second.getDis().read());
            }
            finally {
                pool.discard(second);
                pool.close();
            }
        }
    }

    @Test
    void cacheReadUnblocksWhenTheRemoteClosesMidBatch() throws Exception {
        try (FakeRemote remote = new FakeRemote(socket -> {
            DataInputStream in = input(socket);
            in.read();
            in.readInt();
            socket.close();
        })) {
            Connection connection = new Connection("127.0.0.1", remote.port(), null);
            CachedRecordSet records = new CachedRecordSet(4, connection, false, CompareMode.MISSING_RECORDS);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<Void> read = executor.submit(() -> {
                while (records.next()) {
                    // drain until the failure
                }
                return null;
            });
            try {
                ExecutionException failure = assertThrows(ExecutionException.class, () -> read.get(5, TimeUnit.SECONDS));
                assertInstanceOf(AerospikeException.class, failure.getCause());
                assertFalse(records.close());
            }
            finally {
                read.cancel(true);
                executor.shutdownNow();
                connection.close();
            }
        }
    }

    @Test
    void closingTheCacheMidScanStillFinishesTheBatchBeforeReusingTheSocket() throws Exception {
        AtomicInteger closeCommand = new AtomicInteger(-1);
        AtomicInteger trailing = new AtomicInteger(-1);
        CountDownLatch sawTrailing = new CountDownLatch(1);
        int cacheSize = 4;
        try (FakeRemote remote = new FakeRemote(socket -> {
            DataInputStream in = input(socket);
            DataOutputStream out = output(socket);
            assertEquals(RemoteServer.CMD_RS_MULTI_KEY_ONLY, in.read());
            assertEquals(cacheSize, in.readInt());
            for (int i = 0; i < cacheSize; i++) {
                out.writeBoolean(true);
                RemoteUtils.sendKey(new Key("test", "users", "k" + i), out);
            }
            out.flush();
            closeCommand.set(in.read());
            out.writeInt(0);
            out.flush();
            trailing.set(in.readInt());
            sawTrailing.countDown();
        })) {
            Connection connection = new Connection("127.0.0.1", remote.port(), null);
            CachedRecordSet records = new CachedRecordSet(cacheSize, connection, false, CompareMode.MISSING_RECORDS);
            assertTrue(records.next());
            assertEquals("k0", records.getKey().userKey.getObject());
            assertTrue(records.close());
            connection.getDos().writeInt(12345);
            connection.getDos().flush();
            assertTrue(sawTrailing.await(5, TimeUnit.SECONDS));
            connection.close();
        }
        assertEquals(RemoteServer.CMD_RS_CLOSE, closeCommand.get());
        assertEquals(12345, trailing.get());
    }

    @Test
    void connectionDisablesNaglesAlgorithm() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            Thread accept = new Thread(() -> {
                try {
                    server.accept().close();
                }
                catch (IOException ignored) {
                }
            });
            accept.setDaemon(true);
            accept.start();
            Connection connection = new Connection("127.0.0.1", server.getLocalPort(), null);
            try {
                Field socketField = Connection.class.getDeclaredField("socket");
                socketField.setAccessible(true);
                assertTrue(((Socket) socketField.get(connection)).getTcpNoDelay());
            }
            finally {
                connection.close();
            }
        }
    }

    @Test
    void partitionQueryStopsWhenTheClientDisconnects() throws Exception {
        int port;
        try (ServerSocket probe = new ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        RemoteServer server = new RemoteServer(new EmptyQueryClient(), port, -1, false, false);
        Thread accept = new Thread(() -> {
            try {
                server.start(null);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        });
        accept.setDaemon(true);
        accept.start();

        try (Socket socket = connect(port)) {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());
            out.write(RemoteServer.CMD_QUERY_PARTITION);
            out.writeInt(1);
            out.writeBoolean(false);
            out.writeBoolean(false);
            out.writeInt(0);
            out.writeUTF("test");
            out.writeUTF("");
            out.writeInt(0);
            out.writeInt(0);
            out.writeInt(1);
            out.flush();
            assertEquals("Ready", in.readUTF());
        }

        long deadline = System.currentTimeMillis() + 2000;
        while (System.currentTimeMillis() < deadline) {
            boolean handlerAlive = Thread.getAllStackTraces().keySet().stream()
                    .anyMatch(thread -> thread.getName().startsWith("remote-client") && thread.isAlive());
            if (!handlerAlive) {
                return;
            }
            Thread.sleep(20);
        }
        fail("remote client handler was still running after the client disconnected");
    }

    private static Socket connect(int port) throws IOException, InterruptedException {
        IOException last = null;
        for (int attempt = 0; attempt < 50; attempt++) {
            try {
                return new Socket("127.0.0.1", port);
            }
            catch (IOException ioe) {
                last = ioe;
                Thread.sleep(20);
            }
        }
        throw last;
    }

    private static RemoteAerospikeClient client(FakeRemote remote) throws Exception {
        ClusterComparatorOptions options = new ClusterComparatorOptions(new String[] {"--namespaces", "test"}, true);
        return new RemoteAerospikeClient("127.0.0.1", remote.port(), 1, null, options);
    }

    private static DataInputStream input(Socket socket) throws IOException {
        return new DataInputStream(socket.getInputStream());
    }

    private static DataOutputStream output(Socket socket) throws IOException {
        return new DataOutputStream(socket.getOutputStream());
    }

    private static void expectConfig(DataInputStream in, DataOutputStream out) throws IOException {
        assertEquals(RemoteServer.CMD_CONFIG, in.read());
        in.readBoolean();
        out.writeInt(0);
        out.flush();
    }

    private static void expectClose(DataInputStream in, DataOutputStream out) throws IOException {
        assertEquals(RemoteServer.CMD_CLOSE, in.read());
        out.writeInt(0);
        out.flush();
    }

    private static DataInputStream utf(String value) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeUTF(value);
        out.flush();
        return new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
    }

    @FunctionalInterface
    private interface SocketHandler {
        void handle(Socket socket) throws IOException;
    }

    /**
     * Accepts connections until closed. Each accepted socket is handled on this thread, then the next is accepted.
     */
    private static final class FakeRemote implements AutoCloseable {
        private final ServerSocket serverSocket;
        private final Thread thread;
        private volatile IOException failure;

        FakeRemote(SocketHandler handler) throws IOException {
            this.serverSocket = new ServerSocket(0);
            this.thread = new Thread(() -> {
                while (!serverSocket.isClosed()) {
                    final Socket socket;
                    try {
                        socket = serverSocket.accept();
                    }
                    catch (IOException ioe) {
                        if (!serverSocket.isClosed()) {
                            failure = ioe;
                        }
                        break;
                    }
                    Thread worker = new Thread(() -> {
                        try (Socket accepted = socket) {
                            accepted.setTcpNoDelay(true);
                            accepted.setSoTimeout(5000);
                            handler.handle(accepted);
                        }
                        catch (IOException ioe) {
                            if (!serverSocket.isClosed()) {
                                failure = ioe;
                            }
                        }
                    }, "fake-remote-conn");
                    worker.setDaemon(true);
                    worker.start();
                }
            }, "fake-remote");
            this.thread.setDaemon(true);
            this.thread.start();
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        @Override
        public void close() throws IOException {
            serverSocket.close();
            try {
                thread.join(2000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static final class EmptyRecords implements RecordSetAccess {
        @Override
        public boolean next() {
            return false;
        }

        @Override
        public Key getKey() {
            return null;
        }

        @Override
        public Record getRecord() {
            return null;
        }

        @Override
        public byte[] getRecordHash(boolean sortMaps) {
            return null;
        }

        @Override
        public void close() {
        }
    }

    private static final class EmptyQueryClient implements AerospikeClientAccess {
        @Override
        public RecordSetAccess queryPartitions(QueryPolicy queryPolicy, Statement statement, PartitionFilter filter) {
            return new EmptyRecords();
        }

        @Override
        public boolean isLocal() {
            return true;
        }

        @Override
        public void close() {
        }

        @Override
        public void put(WritePolicy policy, Key key, Bin... bins) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void touch(WritePolicy policy, Key key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(WritePolicy policy, Key key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(Policy policy, Key key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Record get(Policy policy, Key key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean[] exists(BatchPolicy policy, Key[] keys) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Record[] get(BatchPolicy policy, Key[] keys) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RecordMetadata getMetadata(WritePolicy policy, Key key) {
            return null;
        }

        @Override
        public Map<String, String> invokeInfoCommandOnAllNodes(String info) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String invokeInfoCommandOnANode(String info) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getNodeNames() {
            throw new UnsupportedOperationException();
        }
    }
}

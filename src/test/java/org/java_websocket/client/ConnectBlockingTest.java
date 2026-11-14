package org.java_websocket.client;

import java.io.IOException;
import java.net.*;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.*;
import org.java_websocket.client.*;
import org.java_websocket.server.WebSocketServer;
import org.java_websocket.util.SocketUtil;
import org.java_websocket.enums.ReadyState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;

public class ConnectBlockingTest {

    @Test
    @Timeout(1000)
    public void test_ConnectBlockingCleanup() throws Throwable {

        Set<Thread> threadSet1 = Thread.getAllStackTraces().keySet();
        final CountDownLatch accepted = new CountDownLatch(1);

        final int port = SocketUtil.getAvailablePort();

        // Keep both server sockets referenced until the end of the test: a garbage collection
        // closes an unreachable socket, and the client would then fail on the closed connection
        // instead of timing out.
        final ServerSocket serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        final AtomicReference<Socket> clientSocket = new AtomicReference<>();

        /* TCP server which listens to a port, but does not answer handshake */
        Thread server = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    clientSocket.set(serverSocket.accept());
                    accepted.countDown();
                } catch (Throwable t) {
                    assertInstanceOf(InterruptedException.class, t);
                }
            }
        });
        server.start();

        WebSocketClient client = new WebSocketClient(URI.create("ws://localhost:" + port)) {
            @Override
            public void onOpen(ServerHandshake handshake) {
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
            }

            @Override
            public void onMessage(String message) {
            }

            @Override
            public void onError(Exception ex) {
                ex.printStackTrace();
            }
        };
        boolean connected = client.connectBlocking(100, TimeUnit.MILLISECONDS);
        assertEquals( 0, accepted.getCount(), "TCP socket should have been accepted");
        assertFalse(connected, "WebSocket should not be connected (as server didn't send handshake)");

        server.interrupt();
        server.join();

        Set<Thread> threadSet2 = new HashSet<>(Thread.getAllStackTraces().keySet());
        threadSet2.removeAll(threadSet1);
        assertEquals(Collections.emptySet(), threadSet2, "no threads left over");
        assertTrue(client.getReadyState() == ReadyState.CLOSED || client.getReadyState() == ReadyState.NOT_YET_CONNECTED, "WebSocket is in closed state");
        clientSocket.get().close();
        serverSocket.close();
    }
}

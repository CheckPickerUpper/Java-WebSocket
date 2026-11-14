package org.java_websocket.issues;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.java_websocket.util.SocketUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class Issue834Test {

  @Test
  @Timeout(1000)
  public void testNoNewThreads() throws InterruptedException {

    Set<Thread> threadSet1 = Thread.getAllStackTraces().keySet();

    new WebSocketServer(new InetSocketAddress(SocketUtil.getAvailablePort())) {
      @Override
      public void onOpen(WebSocket conn, ClientHandshake handshake) {
      }

      @Override
      public void onClose(WebSocket conn, int code, String reason, boolean remote) {
      }

      @Override
      public void onMessage(WebSocket conn, String message) {
      }

      @Override
      public void onError(WebSocket conn, Exception ex) {
      }

      @Override
      public void onStart() {
      }
    };

    Set<Thread> threadSet2 = new HashSet<>(Thread.getAllStackTraces().keySet());
    threadSet2.removeAll(threadSet1);

    //checks that no threads are started in the constructor
    assertEquals(Collections.emptySet(), threadSet2);

  }

}

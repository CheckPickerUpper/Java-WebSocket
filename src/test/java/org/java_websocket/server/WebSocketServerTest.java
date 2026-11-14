/*
 * Copyright (c) 2010-2020 Nathan Rajlich
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 *
 */

package org.java_websocket.server;


import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.java_websocket.WebSocket;
import org.java_websocket.drafts.Draft;
import org.java_websocket.drafts.Draft_6455;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.util.Charsetfunctions;
import org.java_websocket.util.SocketUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;

public class WebSocketServerTest {

  @Test
  public void testConstructor() {
    List<Draft> draftCollection = Collections.<Draft>singletonList(new Draft_6455());
    Collection<WebSocket> webSocketCollection = new HashSet<WebSocket>();
    InetSocketAddress inetAddress = new InetSocketAddress(1337);

    try {
      WebSocketServer server = new MyWebSocketServer(null, 1, draftCollection, webSocketCollection);
      fail("Should fail");
    } catch (IllegalArgumentException e) {
      //OK
    }
    try {
      WebSocketServer server = new MyWebSocketServer(inetAddress, 0, draftCollection,
          webSocketCollection);
      fail("Should fail");
    } catch (IllegalArgumentException e) {
      //OK
    }
    try {
      WebSocketServer server = new MyWebSocketServer(inetAddress, -1, draftCollection,
          webSocketCollection);
      fail("Should fail");
    } catch (IllegalArgumentException e) {
      //OK
    }
    try {
      WebSocketServer server = new MyWebSocketServer(inetAddress, Integer.MIN_VALUE,
          draftCollection, webSocketCollection);
      fail("Should fail");
    } catch (IllegalArgumentException e) {
      //OK
    }
    try {
      WebSocketServer server = new MyWebSocketServer(inetAddress, Integer.MIN_VALUE,
          draftCollection, webSocketCollection);
      fail("Should fail");
    } catch (IllegalArgumentException e) {
      //OK
    }
    try {
      WebSocketServer server = new MyWebSocketServer(inetAddress, 1, draftCollection, null);
      fail("Should fail");
    } catch (IllegalArgumentException e) {
      //OK
    }

    try {
      WebSocketServer server = new MyWebSocketServer(inetAddress, 1, draftCollection,
          webSocketCollection);
      // OK
    } catch (IllegalArgumentException e) {
      fail("Should not fail");
    }
    try {
      WebSocketServer server = new MyWebSocketServer(inetAddress, 1, null, webSocketCollection);
      // OK
    } catch (IllegalArgumentException e) {
      fail("Should not fail");
    }
  }


  @Test
  public void testGetAddress() throws  InterruptedException {
    int port = SocketUtil.getAvailablePort();
    InetSocketAddress inetSocketAddress = new InetSocketAddress(port);
    MyWebSocketServer server = new MyWebSocketServer(port);
    assertEquals(inetSocketAddress, server.getAddress());
  }

  @Test
  public void testGetDrafts() {
    List<Draft> draftCollection = Collections.<Draft>singletonList(new Draft_6455());
    Collection<WebSocket> webSocketCollection = new HashSet<WebSocket>();
    InetSocketAddress inetAddress = new InetSocketAddress(1337);
    MyWebSocketServer server = new MyWebSocketServer(inetAddress, 1, draftCollection,
        webSocketCollection);
    assertEquals(1, server.getDraft().size());
    assertEquals(draftCollection.get(0), server.getDraft().get(0));
  }

  @Test
  public void testGetPort() throws IOException, InterruptedException {
    int port = SocketUtil.getAvailablePort();
    CountDownLatch countServerDownLatch = new CountDownLatch(1);
    MyWebSocketServer server = new MyWebSocketServer(port);
    assertEquals(port, server.getPort());
    server = new MyWebSocketServer(0, countServerDownLatch);
    assertEquals(0, server.getPort());
    server.start();
    countServerDownLatch.await();
    assertNotEquals(0, server.getPort());
  }

  @Test
  public void testMaxPendingConnections() {
    MyWebSocketServer server = new MyWebSocketServer(1337);
    assertEquals(-1, server.getMaxPendingConnections());
    server.setMaxPendingConnections(10);
    assertEquals(10, server.getMaxPendingConnections());
  }

  @Test
  public void testBroadcast() {
    MyWebSocketServer server = new MyWebSocketServer(1337);
    try {
      server.broadcast((byte[]) null, Collections.<WebSocket>emptyList());
      fail("Should fail");
    } catch (IllegalArgumentException e) {
      // OK
    }
    try {
      server.broadcast((ByteBuffer) null, Collections.<WebSocket>emptyList());
      fail("Should fail");
    } catch (IllegalArgumentException e) {
      // OK
    }
    try {
      server.broadcast((String) null, Collections.<WebSocket>emptyList());
      fail("Should fail");
    } catch (IllegalArgumentException e) {
      // OK
    }
    try {
      server.broadcast(new byte[]{(byte) 0xD0}, null);
      fail("Should fail");
    } catch (IllegalArgumentException e) {
      // OK
    }
    try {
      server.broadcast(ByteBuffer.wrap(new byte[]{(byte) 0xD0}), null);
      fail("Should fail");
    } catch (IllegalArgumentException e) {
      // OK
    }
    try {
      server.broadcast("", null);
      fail("Should fail");
    } catch (IllegalArgumentException e) {
      // OK
    }
    try {
      server.broadcast("", Collections.<WebSocket>emptyList());
      // OK
    } catch (IllegalArgumentException e) {
      fail("Should not fail");
    }
  }

  private static class MyWebSocketServer extends WebSocketServer {

    private CountDownLatch serverLatch = null;

    public MyWebSocketServer(InetSocketAddress address, int decodercount, List<Draft> drafts,
        Collection<WebSocket> connectionscontainer) {
      super(address, decodercount, drafts, connectionscontainer);
    }

    public MyWebSocketServer(int port, CountDownLatch serverLatch) {
      super(new InetSocketAddress(port));
      this.serverLatch = serverLatch;
    }

    public MyWebSocketServer(int port) {
      this(port, null);
    }

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
      ex.printStackTrace();
    }

    @Override
    public void onStart() {
      if (serverLatch != null) {
        serverLatch.countDown();
      }
    }
  }

  @Test
  @Timeout(30)
  public void testStopClosesConnectionWhoseCloseFrameWasNotWritten() throws Exception {
    int port = SocketUtil.getAvailablePort();
    final CountDownLatch serverStarted = new CountDownLatch(1);
    final CountDownLatch serverOpened = new CountDownLatch(1);
    final AtomicReference<WebSocket> serverConnection = new AtomicReference<>();
    WebSocketServer server = new WebSocketServer(new InetSocketAddress(port)) {
      @Override
      public void onOpen(WebSocket conn, ClientHandshake handshake) {
        serverConnection.set(conn);
        serverOpened.countDown();
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
        serverStarted.countDown();
      }
    };
    server.start();
    serverStarted.await();
    try (Socket client = new Socket()) {
      // The client reads nothing until the server has stopped, and its small receive buffer keeps
      // the server from writing more than a few megabytes of the message below, so the server's
      // close frame stays queued behind the rest of the message.
      client.setReceiveBufferSize(4096);
      client.connect(new InetSocketAddress("localhost", port));
      client.getOutputStream().write(Charsetfunctions.asciiBytes("GET / HTTP/1.1\r\n"
          + "Host: localhost:" + port + "\r\n"
          + "Upgrade: websocket\r\n"
          + "Connection: Upgrade\r\n"
          + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
          + "Sec-WebSocket-Version: 13\r\n\r\n"));
      serverOpened.await();
      serverConnection.get().send(ByteBuffer.allocate(32 * 1024 * 1024));
      server.stop();
      client.setSoTimeout(5000);
      InputStream in = client.getInputStream();
      byte[] buffer = new byte[64 * 1024];
      try {
        while (in.read(buffer) != -1) {
          // read what the server wrote before it stopped, until the server closes the connection
        }
      } catch (SocketTimeoutException e) {
        fail("the server stopped but left the connection open");
      }
    }
  }
}


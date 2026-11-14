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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.java_websocket.WebSocket;
import org.java_websocket.WebSocketAdapter;
import org.java_websocket.WebSocketImpl;
import org.java_websocket.WebSocketServerFactory;
import org.java_websocket.WrappedByteChannel;
import org.java_websocket.drafts.Draft;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.util.SocketUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class WebSocketServerWriteDemandTest {

  private static final byte[] PROBE = "queued during write".getBytes(StandardCharsets.US_ASCII);

  @Test
  @Timeout(10)
  public void writesDataQueuedWhileTheSelectorClearsWriteInterest() throws Exception {
    int port = SocketUtil.getAvailablePort();
    CountDownLatch started = new CountDownLatch(1);
    WebSocketServer server = new WebSocketServer(new InetSocketAddress(port)) {
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
        started.countDown();
      }
    };
    server.setWebSocketFactory(new QueueDuringBatchFactory(server));
    server.start();
    try {
      started.await();
      try (Socket socket = new Socket("localhost", port)) {
        socket.setSoTimeout(5000);
        // The start of an opening handshake: the server reads it and waits for the rest.
        socket.getOutputStream().write("GET / HTTP/1.1\r\n".getBytes(StandardCharsets.US_ASCII));
        InputStream in = socket.getInputStream();
        byte[] received = new byte[PROBE.length];
        int filled = 0;
        try {
          while (filled < received.length) {
            int count = in.read(received, filled, received.length - filled);
            if (count < 0) {
              fail("connection closed after " + filled + " of " + PROBE.length + " queued bytes");
            }
            filled += count;
          }
        } catch (SocketTimeoutException e) {
          fail("the server never wrote the bytes queued while the selector cleared OP_WRITE; "
              + "received " + filled + " of " + PROBE.length, e);
        }
        assertArrayEquals(PROBE, received);
      }
    } finally {
      server.stop();
    }
  }

  private static final class QueueDuringBatchFactory implements WebSocketServerFactory {

    private final DefaultWebSocketServerFactory delegate = new DefaultWebSocketServerFactory();
    private final WebSocketServer server;

    QueueDuringBatchFactory(WebSocketServer server) {
      this.server = server;
    }

    @Override
    public WebSocketImpl createWebSocket(WebSocketAdapter a, Draft d) {
      return delegate.createWebSocket(a, d);
    }

    @Override
    public WebSocketImpl createWebSocket(WebSocketAdapter a, List<Draft> drafts) {
      return delegate.createWebSocket(a, drafts);
    }

    @Override
    public ByteChannel wrapChannel(SocketChannel channel, SelectionKey key) {
      return new QueueDuringBatchChannel(channel, key, server);
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

  /**
   * Queues data and requests OP_WRITE, as a worker thread writing to the connection does, from
   * inside {@link org.java_websocket.SocketChannelIOHelper#batch}, after batch has found the
   * queue empty.
   *
   * <p>Until the client's first bytes are read, this channel reports a pending write, as
   * SSLSocketChannel2 does during its handshake, so OP_WRITE stays set. The data is queued in the
   * first write after a read: that read removed the key from the selected-key set, so the selector
   * reaches the key again only through its interest set.
   */
  private static final class QueueDuringBatchChannel implements WrappedByteChannel {

    private final SocketChannel channel;
    private final SelectionKey key;
    private final WebSocketServer server;
    private boolean read;
    private boolean queued;

    QueueDuringBatchChannel(SocketChannel channel, SelectionKey key, WebSocketServer server) {
      this.channel = channel;
      this.key = key;
      this.server = server;
      key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
    }

    @Override
    public boolean isNeedWrite() {
      if (!read) {
        return true;
      }
      if (!queued) {
        queued = true;
        WebSocketImpl conn = (WebSocketImpl) key.attachment();
        conn.outQueue.add(ByteBuffer.wrap(PROBE));
        server.onWriteDemand(conn);
      }
      return false;
    }

    @Override
    public void writeMore() {
    }

    @Override
    public boolean isNeedRead() {
      return false;
    }

    @Override
    public int readMore(ByteBuffer dst) {
      return 0;
    }

    @Override
    public boolean isBlocking() {
      return false;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      int count = channel.read(dst);
      if (count > 0) {
        read = true;
      }
      return count;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      return channel.write(src);
    }

    @Override
    public boolean isOpen() {
      return channel.isOpen();
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }
  }
}

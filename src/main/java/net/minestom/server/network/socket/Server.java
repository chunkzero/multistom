package net.minestom.server.network.socket;

import net.minestom.server.ProcessOwned;
import net.minestom.server.ServerProcess;
import net.minestom.server.network.packet.PacketParser;
import net.minestom.server.network.player.PlayerSocketConnection;
import net.minestom.server.thread.TickSchedulerThread;
import net.minestom.server.thread.TickThread;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.UnknownNullability;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ProtocolFamily;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public final class Server implements ProcessOwned {
    private volatile ConnectionFactory connectionFactory;
    private volatile boolean stop;
    private final Set<PlayerSocketConnection> connections = new HashSet<>();
    private @UnknownNullability Thread acceptThread;

    private final PacketParser.Client packetParser;
    private final ServerProcess process;

    private volatile @UnknownNullability ServerSocketChannel serverSocket;
    private @UnknownNullability SocketAddress socketAddress;
    private @UnknownNullability String address;
    private int port;

    public Server(ServerProcess process, PacketParser.Client packetParser) {
        this.process = Objects.requireNonNull(process);
        this.packetParser = packetParser;
        this.connectionFactory = (channel, remoteAddress, readThread, writeThread) ->
                new PlayerSocketConnection(process, channel, remoteAddress, readThread, writeThread);
    }

    @Override
    public ServerProcess process() {
        return process;
    }

    @ApiStatus.Internal
    public synchronized void init(SocketAddress address) throws IOException {
        if (stop) throw new IllegalStateException("Server is closed");
        if (serverSocket != null) throw new IllegalStateException("Server is already bound");
        ProtocolFamily family;
        switch (address) {
            case InetSocketAddress inetSocketAddress -> {
                this.address = inetSocketAddress.getHostString();
                this.port = inetSocketAddress.getPort();
                family = inetSocketAddress.getAddress().getAddress().length == 4 ? StandardProtocolFamily.INET : StandardProtocolFamily.INET6;
            }
            case UnixDomainSocketAddress unixDomainSocketAddress -> {
                this.address = "unix://" + unixDomainSocketAddress.getPath();
                this.port = 0;
                family = StandardProtocolFamily.UNIX;
            }
            default ->
                    throw new IllegalArgumentException("Address must be an InetSocketAddress or a UnixDomainSocketAddress");
        }

        ServerSocketChannel server = ServerSocketChannel.open(family);
        try {
            server.bind(address);
        } catch (IOException | RuntimeException | Error failure) {
            try {
                server.close();
            } catch (IOException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
        this.serverSocket = server;
        this.socketAddress = address;

        if (address instanceof InetSocketAddress && port == 0) {
            port = server.socket().getLocalPort();
        }
    }

    @ApiStatus.Internal
    public synchronized void start() {
        if (stop) throw new IllegalStateException("Server is closed");
        if (acceptThread != null) throw new IllegalStateException("Server already started");
        final var serverSocket = Objects.requireNonNull(this.serverSocket, "Server is not bound");
        acceptThread = Thread.ofVirtual().name("Ms-Socket-Server-" + process.id()).unstarted(() -> {
            // Use named thread builders for logging
            var readBuilder = Thread.ofVirtual().name("Ms-Socket-Reader-" + process.id() + "-", 0);
            var writeBuilder = Thread.ofVirtual().name("Ms-Socket-Writer-" + process.id() + "-", 0);
            while (!stop) {
                final SocketChannel client;
                try {
                    client = serverSocket.accept();
                } catch (ClosedChannelException _) {
                    break; // We are exiting, bye bye!
                } catch (IOException e) {
                    if (!process().properties().suppressConnectionAcceptErrors().get())
                        process().exceptionManager().handleException(e);
                    continue;
                }

                AtomicReference<@UnknownNullability PlayerSocketConnection> reference = new AtomicReference<>(null);
                try {
                    configureSocket(client);
                    Thread readThread = readBuilder.unstarted(() -> playerReadLoop(reference.get()));
                    Thread writeThread = writeBuilder.unstarted(() -> playerWriteLoop(reference.get()));
                    PlayerSocketConnection connection = connectionFactory.create(client, client.getRemoteAddress(), readThread, writeThread);
                    if (connection == null) {
                        client.close(); // Rejected by the factory
                        continue;
                    }
                    if (connection.process() != process())
                        throw new IllegalStateException("Connection belongs to another process");
                    reference.set(connection);
                    synchronized (this) {
                        if (stop) {
                            client.close();
                            continue;
                        }
                        connections.add(connection);
                        try {
                            readThread.start();
                            writeThread.start();
                        } catch (RuntimeException | Error failure) {
                            connections.remove(connection);
                            throw failure;
                        }
                    }
                } catch (IOException | RuntimeException | Error e) {
                    try {
                        client.close();
                    } catch (IOException cleanupFailure) {
                        e.addSuppressed(cleanupFailure);
                    }
                    if (!stop && !process().properties().suppressConnectionAcceptErrors().get())
                        process().exceptionManager().handleException(e);
                }
            }
        });
        acceptThread.start();
    }

    private void configureSocket(SocketChannel channel) throws IOException {
        if (channel.getLocalAddress() instanceof InetSocketAddress) {
            Socket socket = channel.socket();
            socket.setSendBufferSize(process().properties().socketSendBufferSize().get());
            socket.setReceiveBufferSize(process().properties().socketReceiveBufferSize().get());
            socket.setTcpNoDelay(process().properties().socketNoDelay().get());
            socket.setSoTimeout(process().properties().socketTimeout().get());
        }
    }

    private void playerReadLoop(PlayerSocketConnection connection) {
        Objects.requireNonNull(connection, "connection cannot be null");
        try {
            while (!stop && connection.isOnline()) connection.read(packetParser);
        } catch (ClosedChannelException | EOFException _) {
            // The peer or shutdown closed the connection.
        } catch (IOException e) {
            if (!stop && !process().properties().suppressConnectionIoErrors().get())
                process.exceptionManager().handleException(e);
        } catch (Throwable e) {
            if (!stop) process.exceptionManager().handleException(e);
        } finally {
            connection.disconnect();
        }
    }

    private void playerWriteLoop(PlayerSocketConnection connection) {
        try {
            while (!stop && connection.isOnline()) connection.flushSync();
            if (!connection.isOnline()) connection.flushSync(); // Drain the final disconnect packet.
        } catch (ClosedChannelException | EOFException _) {
            // The peer or shutdown closed the connection.
        } catch (IOException e) {
            if (!stop && !process().properties().suppressConnectionIoErrors().get())
                process.exceptionManager().handleException(e);
        } catch (Throwable e) {
            if (!stop) process.exceptionManager().handleException(e);
        } finally {
            try {
                connection.disconnect();
            } finally {
                try {
                    connection.getChannel().close();
                } catch (IOException e) {
                    if (!stop) process.exceptionManager().handleException(e);
                } finally {
                    connection.cleanup();
                    synchronized (this) {
                        connections.remove(connection);
                    }
                }
            }
        }
    }

    public boolean isOpen() {
        var socket = serverSocket;
        return !stop && socket != null && socket.isOpen();
    }

    public void stop() {
        final List<PlayerSocketConnection> connections;
        synchronized (this) {
            if (stop) return;
            stop = true;
            connections = List.copyOf(this.connections);
        }
        var failures = new ArrayList<Throwable>();
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (Throwable failure) {
            failures.add(failure);
        }
        for (var connection : connections) {
            try {
                connection.disconnect();
            } catch (Throwable failure) {
                failures.add(failure);
            }
        }
        final long drainDeadline = System.nanoTime() + Duration.ofMillis(100).toNanos();
        for (var connection : connections) {
            try {
                long remaining = drainDeadline - System.nanoTime();
                if (remaining > 0 && connection.writeThread() != Thread.currentThread())
                    connection.writeThread().join(Duration.ofNanos(remaining));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                failures.add(interrupted);
            }
            try {
                connection.getChannel().close();
            } catch (Throwable failure) {
                failures.add(failure);
            }
            if (connection.readThread() != Thread.currentThread()) connection.readThread().interrupt();
            if (connection.writeThread() != Thread.currentThread()) connection.writeThread().interrupt();
        }
        // Disconnect callbacks may acquire the calling tick worker.
        if (!(Thread.currentThread() instanceof TickThread) && !(Thread.currentThread() instanceof TickSchedulerThread)) {
            final long joinDeadline = System.nanoTime() + Duration.ofSeconds(1).toNanos();
            var threads = new ArrayList<Thread>();
            if (acceptThread != null) threads.add(acceptThread);
            for (var connection : connections) {
                threads.add(connection.readThread());
                threads.add(connection.writeThread());
            }
            try {
                for (var thread : threads) {
                    if (thread == Thread.currentThread() || !thread.isAlive()) continue;
                    final long remaining = joinDeadline - System.nanoTime();
                    if (remaining > 0) thread.join(Duration.ofNanos(remaining));
                    if (thread.isAlive()) failures.add(new TimeoutException("Socket thread did not terminate: " + thread.getName()));
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                failures.add(interrupted);
            }
        }
        try {
            if (socketAddress instanceof UnixDomainSocketAddress unixAddress) Files.deleteIfExists(unixAddress.getPath());
        } catch (Throwable failure) {
            failures.add(failure);
        }
        if (!failures.isEmpty()) {
            var failure = new IllegalStateException("Failed to close server sockets");
            failures.forEach(failure::addSuppressed);
            throw failure;
        }
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = Objects.requireNonNull(connectionFactory);
    }

    public ConnectionFactory connectionFactory() {
        return connectionFactory;
    }

    @ApiStatus.Internal
    public PacketParser.Client packetParser() {
        return packetParser;
    }

    public SocketAddress socketAddress() {
        return socketAddress;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }
}

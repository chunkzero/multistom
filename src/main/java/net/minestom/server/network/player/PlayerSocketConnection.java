package net.minestom.server.network.player;

import net.minestom.server.ServerProcess;
import net.minestom.server.entity.GameMode;
import net.minestom.server.entity.Player;
import net.minestom.server.event.ListenerHandle;
import net.minestom.server.event.player.PlayerPacketOutEvent;
import net.minestom.server.extras.mojangAuth.MojangCrypt;
import net.minestom.server.network.ConnectionState;
import net.minestom.server.network.NetworkBuffer;
import net.minestom.server.network.packet.PacketEncodingContext;
import net.minestom.server.network.packet.PacketParser;
import net.minestom.server.network.packet.PacketReading;
import net.minestom.server.network.packet.PacketRegistry;
import net.minestom.server.network.packet.PacketVanilla;
import net.minestom.server.network.packet.PacketWriting;
import net.minestom.server.network.packet.client.ClientPacket;
import net.minestom.server.network.packet.client.common.ClientCookieResponsePacket;
import net.minestom.server.network.packet.client.common.ClientKeepAlivePacket;
import net.minestom.server.network.packet.client.common.ClientPingRequestPacket;
import net.minestom.server.network.packet.client.configuration.ClientFinishConfigurationPacket;
import net.minestom.server.network.packet.client.configuration.ClientSelectKnownPacksPacket;
import net.minestom.server.network.packet.client.handshake.ClientHandshakePacket;
import net.minestom.server.network.packet.client.login.ClientEncryptionResponsePacket;
import net.minestom.server.network.packet.client.login.ClientLoginAcknowledgedPacket;
import net.minestom.server.network.packet.client.login.ClientLoginPluginResponsePacket;
import net.minestom.server.network.packet.client.login.ClientLoginStartPacket;
import net.minestom.server.network.packet.client.play.ClientConfigurationAckPacket;
import net.minestom.server.network.packet.client.play.ClientCreativeInventoryActionPacket;
import net.minestom.server.network.packet.client.status.StatusRequestPacket;
import net.minestom.server.network.packet.server.BufferedPacket;
import net.minestom.server.network.packet.server.CachedPacket;
import net.minestom.server.network.packet.server.FramedPacket;
import net.minestom.server.network.packet.server.SendablePacket;
import net.minestom.server.network.packet.server.ServerPacket;
import net.minestom.server.network.packet.server.login.SetCompressionPacket;
import net.minestom.server.property.ServerProperties;
import net.minestom.server.utils.collection.ConcurrentMessageQueues;
import net.minestom.server.utils.validate.Check;
import org.jctools.queues.MessagePassingQueue;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;

/**
 * Represents a socket connection.
 * <p>
 * It is the implementation used for all network client.
 */
public class PlayerSocketConnection extends PlayerConnection {
    private static final Set<Class<? extends ClientPacket>> IMMEDIATE_PROCESS_PACKETS = Set.of(
            ClientHandshakePacket.class, // First received packet
            ClientCookieResponsePacket.class,
            StatusRequestPacket.class,
            ClientPingRequestPacket.class,
            ClientKeepAlivePacket.class, // Used to calculate latency
            ClientLoginStartPacket.class,
            ClientEncryptionResponsePacket.class, // Auth request
            ClientLoginPluginResponsePacket.class,
            ClientSelectKnownPacksPacket.class, // Immediate answer to server request on config
            ClientLoginAcknowledgedPacket.class, // Handle config state
            ClientConfigurationAckPacket.class, // Re-enter config state before the next socket read
            ClientFinishConfigurationPacket.class // Enter play state
    );

    private final SocketChannel channel;
    private SocketAddress remoteAddress;
    private boolean attemptedProxyProtocolDetection = false;

    //Could be null. Only used for Mojang Auth
    private volatile @Nullable EncryptionContext encryptionContext;
    private byte[] nonce = new byte[4];

    // Data from client packets
    private @Nullable String loginUsername;
    private volatile @Nullable GameProfile gameProfile;
    private @Nullable String serverAddress;
    private int serverPort;
    private int protocolVersion;

    private final NetworkBuffer readBuffer;
    private final MessagePassingQueue<SendablePacket> packetQueue = ConcurrentMessageQueues.mpscUnboundedArrayQueue(1024);
    private final Thread readThread, writeThread;

    private volatile int compressionThreshold;
    // Writer-owned: changes only after the uncompressed SetCompressionPacket has been encoded.
    private int writeCompressionThreshold;

    // Write lock as the default behavior of the writing thread is to park itself
    // Requires process().properties().fasterSocketWrites().get() to be enabled
    private final AtomicBoolean writeSignaled = new AtomicBoolean(false);

    private final ListenerHandle<PlayerPacketOutEvent> outgoing;

    public PlayerSocketConnection(ServerProcess process, SocketChannel channel, SocketAddress remoteAddress,
                                  Thread readThread, Thread writeThread) {
        super(process);
        this.readBuffer = NetworkBuffer.builder(process.properties().pooledBufferSize().get())
                .autoResize(NetworkBuffer.AutoResize.DOUBLE).registry(process.registries()).properties(process.properties()).build();
        this.outgoing = process.eventHandler().getHandle(PlayerPacketOutEvent.class);
        this.channel = channel;
        this.remoteAddress = remoteAddress;
        this.writeThread = writeThread;
        this.readThread = readThread;
    }

    public void read(PacketParser<ClientPacket> packetParser) throws IOException {
        NetworkBuffer readBuffer = this.readBuffer;
        final long writeIndex = readBuffer.writeIndex();
        final int length = readBuffer.readChannel(channel);

        if (process().properties().proxyProtocol().get() && !attemptedProxyProtocolDetection) {
            final ProxyProtocolDecoder.Result result = ProxyProtocolDecoder.parse(remoteAddress, readBuffer);
            if (result.status() == ProxyProtocolDecoder.Status.NEED_MORE) return;
            attemptedProxyProtocolDetection = true;
            if (result.status() == ProxyProtocolDecoder.Status.PRESENT) {
                this.remoteAddress = result.clientAddress();
            } else if (process().properties().proxyProtocolRequired().get()) {
                throw new IOException("Missing required PROXY protocol header");
            }
        }

        // Decrypt newly read data
        final EncryptionContext encryptionContext = this.encryptionContext;
        if (encryptionContext != null) {
            readBuffer.cipher(encryptionContext.decrypt(), writeIndex, length);
        }
        // Process packets
        processPackets(readBuffer, packetParser);
    }

    private boolean compression() {
        return compressionThreshold > 0;
    }

    private void processPackets(NetworkBuffer readBuffer, PacketParser<ClientPacket> packetParser) {
        final ConnectionState startingState = getClientState();
        final PacketReading.Result<ClientPacket> result;
        try {
            result = PacketReading.<ClientPacket>readPackets(
                    readBuffer,
                    packetParser,
                    startingState, PacketVanilla::nextClientState,
                    compression(),
                    this::readClientPacket, process().packetBuffers()
            );
        } catch (Throwable e) {
            // Errors thrown while still in the starting state are usually garbage
            // from scanners. A packet that errors after a state change within the
            // same batch is still checked against the starting state.
            if (startingState.ordinal() > process().properties().suppressMalformedPacketErrorLevel().get())
                process().exceptionManager().handleException(e);
            // The remaining packets of the batch are lost, disconnect to avoid
            // reading from an invalid state.
            if (process().properties().rejectMalformedPacket().get()) disconnect();
            return;
        }
        switch (result) {
            case PacketReading.Result.Success<ClientPacket> success -> {
                for (PacketReading.ParsedPacket<ClientPacket> parsedPacket : success.packets()) {
                    final ClientPacket packet = parsedPacket.packet();

                    try {
                        final boolean processImmediately = IMMEDIATE_PROCESS_PACKETS.contains(packet.getClass());
                        if (processImmediately) {
                            // Interpret the packet using the connection state we received it.
                            process().packetListenerManager().processClientPacket(packet, this);
                        } else {
                            // To be processed during the next player tick
                            final Player player = getPlayer();
                            assert player != null;
                            player.addPacketToQueue(packet);
                        }
                    } catch (Throwable e) {
                        if (startingState.ordinal() > process().properties().suppressMisusedPacketErrorLevel().get())
                            process().exceptionManager().handleException(e);
                        // Packets already in the queue are unaffected.
                        if (process().properties().rejectMisusedPacket().get()) disconnect();
                    }
                }
                // Compact in case of incomplete read
                readBuffer.compact();
            }
            case PacketReading.Result.Empty<ClientPacket> _ -> {
                // Empty
            }
            case PacketReading.Result.Skipped<ClientPacket> _ -> readBuffer.compact();
            case PacketReading.Result.Failure<ClientPacket> failure -> {
                readBuffer.compact(); // Discard any complete frames before resize
                final long requiredCapacity = failure.requiredCapacity();
                if (requiredCapacity > readBuffer.capacity()) {
                    readBuffer.resize(requiredCapacity);
                }
            }
        }
    }

    @Nullable ClientPacket readClientPacket(PacketRegistry.PacketInfo<? extends ClientPacket> packetInfo,
                                            NetworkBuffer buffer) {
        if (packetInfo.packetClass() == ClientCreativeInventoryActionPacket.class) {
            final Player player = getPlayer();
            if (player == null || player.getGameMode() != GameMode.CREATIVE) {
                return null;
            }
        }
        return packetInfo.serializer().read(buffer);
    }

    /**
     * Sets the encryption key and add the codecs to the pipeline.
     *
     * @param secretKey the secret key to use in the encryption
     * @throws IllegalStateException if encryption is already enabled for this connection
     */
    public void setEncryptionKey(SecretKey secretKey) {
        Check.stateCondition(encryptionContext != null, "Encryption is already enabled!");
        this.encryptionContext = new EncryptionContext(MojangCrypt.getCipher(1, secretKey), MojangCrypt.getCipher(2, secretKey));
    }

    /**
     * Negotiates this process's compression threshold. The setting is captured for this connection.
     *
     * @throws IllegalStateException if compression is already enabled or disabled in the process settings
     */
    public synchronized void startCompression() {
        Check.stateCondition(compression(), "Compression is already enabled!");
        final int threshold = process().compressionThreshold();
        Check.stateCondition(threshold <= 0, "Compression is disabled");
        this.compressionThreshold = threshold;
        sendPacket(new SetCompressionPacket(threshold));
    }

    @Override
    public PacketEncodingContext packetContext() {
        return process().packetBuffers().context(getServerState(), compressionThreshold);
    }

    @Override
    public void sendPacket(SendablePacket packet) {
        this.packetQueue.relaxedOffer(packet);
        unlockWriteThread();
    }

    @Override
    public void sendPackets(Collection<? extends SendablePacket> packets) {
        for (SendablePacket packet : packets) this.packetQueue.relaxedOffer(packet);
        unlockWriteThread();
    }

    // Requires process().properties().fasterSocketWrites().get()
    private void unlockWriteThread() {
        if (!process().properties().fasterSocketWrites().get()) return;
        if (!this.writeSignaled.compareAndExchange(false, true)) {
            LockSupport.unpark(writeThread);
        }
    }

    @Override
    public SocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    /**
     * Changes the internal remote address field.
     * <p>
     * Mostly unsafe, used internally when interacting with a proxy.
     *
     * @param remoteAddress the new connection remote address
     */
    @ApiStatus.Internal
    public void setRemoteAddress(SocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public @Nullable GameProfile gameProfile() {
        return gameProfile;
    }

    public void UNSAFE_setProfile(GameProfile gameProfile) {
        this.gameProfile = gameProfile;
    }

    /**
     * Retrieves the username received from the client during connection.
     * <p>
     * This value has not been checked and could be anything.
     *
     * @return the username given by the client, unchecked
     */
    public @Nullable String getLoginUsername() {
        return loginUsername;
    }

    /**
     * Sets the internal login username field.
     *
     * @param loginUsername the new login username field
     */
    public void UNSAFE_setLoginUsername(String loginUsername) {
        this.loginUsername = loginUsername;
    }

    /**
     * Gets the server address that the client used to connect.
     * <p>
     * WARNING: it is given by the client, it is possible for it to be wrong.
     *
     * @return the server address used
     */
    @Override
    public @Nullable String getServerAddress() {
        return serverAddress;
    }

    /**
     * Gets the server port that the client used to connect.
     * <p>
     * WARNING: it is given by the client, it is possible for it to be wrong.
     *
     * @return the server port used
     */
    @Override
    public int getServerPort() {
        return serverPort;
    }

    /**
     * Gets the protocol version of a client.
     *
     * @return protocol version of client.
     */
    @Override
    public int getProtocolVersion() {
        return protocolVersion;
    }

    /**
     * Used in {@link ClientHandshakePacket} to change the internal fields.
     *
     * @param serverAddress   the server address which the client used
     * @param serverPort      the server port which the client used
     * @param protocolVersion the protocol version which the client used
     */
    public void refreshServerInformation(@Nullable String serverAddress, int serverPort, int protocolVersion) {
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
        this.protocolVersion = protocolVersion;
    }

    public byte[] getNonce() {
        return nonce;
    }

    public void setNonce(byte[] nonce) {
        this.nonce = nonce;
    }

    private boolean writeSendable(NetworkBuffer buffer, SendablePacket sendable) {
        final long start = buffer.writeIndex();
        final boolean result = writePacketSync(buffer, sendable);
        if (!result) return false;
        // Encrypt data
        final long length = buffer.writeIndex() - start;
        final EncryptionContext encryptionContext = this.encryptionContext;
        if (encryptionContext != null && length > 0) { // Encryption support
            buffer.cipher(encryptionContext.encrypt(), start, length);
        }
        return true;
    }

    private boolean writePacketSync(NetworkBuffer buffer, SendablePacket packet) {
        final Player player = getPlayer();
        final ConnectionState state = getServerState();
        final var context = process().packetBuffers().context(state, writeCompressionThreshold);
        if (player != null) {
            // Outgoing event
            if (outgoing.hasListener()) {
                final ServerPacket serverPacket = SendablePacket.extractServerPacket(context, packet);
                if (serverPacket != null) { // Events are not called for buffered packets
                    PlayerPacketOutEvent event = new PlayerPacketOutEvent(player, serverPacket);
                    outgoing.call(event);
                    if (event.isCancelled()) return true;
                }
            }
            // Translation
            if (process().properties().automaticComponentTranslation().get() && packet instanceof ServerPacket.ComponentHolding translatablePacket) {
                packet = translatablePacket.copyWithOperator(component ->
                        process().translation().translate(component, player.getLocale()));
            }
        }
        // Write packet
        final long start = buffer.writeIndex();
        try {
            final ServerPacket serverPacket;
            final boolean success;
            switch (packet) {
                case ServerPacket value -> {
                    serverPacket = value;
                    context.write(buffer, value);
                    success = true;
                }
                case FramedPacket framed -> {
                    serverPacket = framed.packet();
                    if (framed.context().equals(context)) {
                        success = writeBuffer(buffer, framed.body(), 0, framed.body().writeIndex());
                    } else {
                        context.write(buffer, serverPacket);
                        success = true;
                    }
                }
                case CachedPacket cached -> {
                    final FramedPacket framed = cached.framed(context);
                    if (framed != null) {
                        serverPacket = framed.packet();
                        success = writeBuffer(buffer, framed.body(), 0, framed.body().writeIndex());
                    } else {
                        serverPacket = cached.packet(context);
                        context.write(buffer, serverPacket);
                        success = true;
                    }
                }
                case BufferedPacket buffered -> {
                    if (buffered.context().buffers() != context.buffers()
                            || buffered.context().compressionThreshold() != context.compressionThreshold())
                        throw new IllegalArgumentException("Buffered packet encoding context does not match connection");
                    // A queued transition can advance the connection past this batch's state.
                    if (buffered.context().state() != state) return true;
                    return writeBuffer(buffer, buffered.buffer(), buffered.index(), buffered.length());
                }
            }
            if (success) {
                setServerState(PacketVanilla.nextServerState(serverPacket, state));
                if (serverPacket instanceof SetCompressionPacket compression) {
                    writeCompressionThreshold = compression.threshold();
                }
            }
            return success;
        } catch (IndexOutOfBoundsException _) {
            buffer.writeIndex(start);
            return false;
        }
    }

    private static boolean writeBuffer(NetworkBuffer buffer, NetworkBuffer body, long index, long length) {
        if (buffer.writableBytes() < length) {
            // Not enough space in the buffer
            return false;
        }
        NetworkBuffer.copy(body, index, buffer, buffer.writeIndex(), length);
        buffer.advanceWrite(length);
        return true;
    }

    private @Nullable NetworkBuffer writeLeftover = null;

    public void flushSync() throws IOException {
        // Write leftover if any
        NetworkBuffer leftover = this.writeLeftover;
        if (leftover != null) {
            final boolean success = leftover.writeChannel(channel);
            if (success) {
                this.writeLeftover = null;
                process().packetBuffers().add(leftover);
            } else {
                // Failed to write the whole leftover, try again next flush
                return;
            }
        }
        // Consume queued packets
        var packetQueue = this.packetQueue;
        if (packetQueue.isEmpty()) {
            if (!process().properties().fasterSocketWrites().get()) {
                try {
                    // Can probably be improved by waking up at the end of the tick
                    // But this work well enough and without additional state.
                    Thread.sleep(1000 / ServerProperties.SERVER_TICKS_PER_SECOND.get() / 2);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                assert this.writeThread == Thread.currentThread(): "writeThread should be the current thread";
                this.writeSignaled.set(false);
                if (!isOnline()) return; // already offline, don't park
                LockSupport.park(this);
                if (packetQueue.isEmpty()) return; // woken by disconnect signal, not by packets
            }
        }
        if (!channel.isConnected()) throw new EOFException("Channel is closed");
        NetworkBuffer buffer = process().packetBuffers().get();
        try {
            PacketWriting.writeQueue(buffer, packetQueue, 1, this::writeSendable);
            if (!buffer.writeChannel(channel)) this.writeLeftover = buffer;
        } finally {
            if (this.writeLeftover != buffer) process().packetBuffers().add(buffer);
        }
    }

    @Override
    public void disconnect() {
        try {
            super.disconnect();
        } finally {
            LockSupport.unpark(writeThread);
        }
    }

    public Thread readThread() {
        return readThread;
    }

    public Thread writeThread() {
        return writeThread;
    }

    @ApiStatus.Internal
    public void cleanup() {
        packetQueue.clear();
        final var writeLeftover = this.writeLeftover;
        if (writeLeftover != null) {
            process().packetBuffers().add(writeLeftover);
            this.writeLeftover = null;
        }
    }

    record EncryptionContext(Cipher encrypt, Cipher decrypt) {
    }
}

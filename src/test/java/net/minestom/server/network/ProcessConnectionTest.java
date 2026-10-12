package net.minestom.server.network;

import net.kyori.adventure.nbt.CompoundBinaryTag;
import net.minestom.server.ServerProcess;
import net.minestom.server.command.builder.Command;
import net.minestom.server.coordinate.Pos;
import net.minestom.server.entity.Player;
import net.minestom.server.event.player.AsyncPlayerConfigurationEvent;
import net.minestom.server.event.player.AsyncPlayerPreLoginEvent;
import net.minestom.server.event.player.OutgoingTransferEvent;
import net.minestom.server.event.player.PlayerDisconnectEvent;
import net.minestom.server.event.player.PlayerLoadedEvent;
import net.minestom.server.event.player.PlayerPacketEvent;
import net.minestom.server.event.player.PlayerSettingsChangeEvent;
import net.minestom.server.event.player.PlayerSpawnEvent;
import net.minestom.server.instance.Instance;
import net.minestom.server.network.packet.PacketWriting;
import net.minestom.server.network.packet.client.common.ClientPingRequestPacket;
import net.minestom.server.network.packet.client.common.ClientSettingsPacket;
import net.minestom.server.network.packet.client.handshake.ClientHandshakePacket;
import net.minestom.server.network.packet.client.login.ClientLoginAcknowledgedPacket;
import net.minestom.server.network.packet.client.play.ClientConfigurationAckPacket;
import net.minestom.server.network.packet.client.play.ClientPlayerLoadedPacket;
import net.minestom.server.network.packet.client.status.StatusRequestPacket;
import net.minestom.server.network.packet.server.CachedPacket;
import net.minestom.server.network.packet.server.ServerPacket;
import net.minestom.server.network.packet.server.common.DisconnectPacket;
import net.minestom.server.network.packet.server.common.PingResponsePacket;
import net.minestom.server.network.packet.server.common.PluginMessagePacket;
import net.minestom.server.network.packet.server.common.TransferPacket;
import net.minestom.server.network.packet.server.configuration.RegistryDataPacket;
import net.minestom.server.network.packet.server.login.LoginSuccessPacket;
import net.minestom.server.network.packet.server.login.SetCompressionPacket;
import net.minestom.server.network.packet.server.play.DeclareCommandsPacket;
import net.minestom.server.network.packet.server.play.JoinGamePacket;
import net.minestom.server.network.packet.server.play.PlayerInfoUpdatePacket;
import net.minestom.server.network.packet.server.play.ServerDifficultyPacket;
import net.minestom.server.network.packet.server.play.StartConfigurationPacket;
import net.minestom.server.network.packet.server.status.ResponsePacket;
import net.minestom.server.network.player.ClientSettings;
import net.minestom.server.network.player.GameProfile;
import net.minestom.server.network.player.PlayerSocketConnection;
import net.minestom.server.property.ServerProperties;
import net.minestom.server.world.Difficulty;
import net.minestom.server.world.DimensionType;
import net.minestom.testing.ServerProcessPair;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcessConnectionTest {
    @Test
    void socketWriterReportsApplicationFailuresWhenIoErrorsAreSuppressed() throws Exception {
        assertTrue(ServerProperties.fromSystemProperties().suppressConnectionIoErrors().get());
        try (var pair = new ServerProcessPair()) {
            var process = pair.first();
            var errors = new LinkedBlockingQueue<Throwable>();
            process.exceptionManager().setExceptionHandler(errors::add);
            var admitted = new CompletableFuture<PlayerSocketConnection>();
            process.eventHandler().addListener(AsyncPlayerPreLoginEvent.class,
                    event -> admitted.complete((PlayerSocketConnection) event.getConnection()));
            process.start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            try (var client = new ProtocolClient(process)) {
                client.login("BadPacket", UUID.randomUUID());
                client.readThrough(LoginSuccessPacket.class);
                var connection = admitted.get(5, TimeUnit.SECONDS);
                var expected = new IllegalArgumentException("packet supplier failed");
                connection.sendPacket(new CachedPacket(() -> { throw expected; }));
                assertSame(expected, errors.poll(5, TimeUnit.SECONDS));
                assertTrue(connection.writeThread().join(Duration.ofSeconds(5)));
                assertFalse(connection.isOnline());
                assertThrows(EOFException.class, client::read);
                assertTrue(errors.isEmpty(), errors::toString);
            }
        }
    }

    @Test
    void shutdownReportsAStuckSocketReaderWithoutWaitingForever() throws Exception {
        try (var pair = new ServerProcessPair()) {
            var process = pair.first();
            var release = new CompletableFuture<Void>();
            var entered = new CompletableFuture<Thread>();
            var errors = new LinkedBlockingQueue<Throwable>();
            process.exceptionManager().setExceptionHandler(errors::add);
            process.connectionManager().setPlayerProvider((connection, profile) -> {
                entered.complete(Thread.currentThread());
                release.join(); // Deliberately ignores the shutdown interrupt.
                return new Player(connection, profile);
            });
            process.start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            try (var client = new ProtocolClient(process)) {
                client.login("Stuck", UUID.randomUUID());
                client.readThrough(LoginSuccessPacket.class);
                client.send(new ClientLoginAcknowledgedPacket());
                var reader = entered.get(5, TimeUnit.SECONDS);
                var closer = Thread.startVirtualThread(process::close);
                try {
                    assertTrue(closer.join(Duration.ofSeconds(3)), "Shutdown waited indefinitely for the reader");
                    assertFalse(process.server().isOpen());
                    assertFalse(process.dispatcher().isAlive());
                    assertThrows(EOFException.class, client::read);
                    var failure = errors.poll(1, TimeUnit.SECONDS);
                    assertNotNull(failure);
                    var socketFailure = onlySuppressed(failure);
                    var timeout = assertInstanceOf(TimeoutException.class, onlySuppressed(socketFailure));
                    assertTrue(timeout.getMessage().contains(reader.getName()));
                    pair.second().ticker().tick(System.nanoTime());
                } finally {
                    release.complete(null);
                    assertTrue(reader.join(Duration.ofSeconds(5)));
                    assertTrue(closer.join(Duration.ofSeconds(5)));
                }
            } finally {
                release.complete(null);
            }
        }
    }

    private static Throwable onlySuppressed(Throwable failure) {
        assertEquals(1, failure.getSuppressed().length);
        return failure.getSuppressed()[0];
    }

    @Test
    void connectionFactoryChoosesRejectsAndCannotAdoptForeignConnections() throws Exception {
        try (var pair = new ServerProcessPair()) {
            var process = pair.first();
            var server = process.server();
            var created = new AtomicInteger();
            server.setConnectionFactory((channel, remoteAddress, readThread, writeThread) -> {
                created.incrementAndGet();
                return new PlayerSocketConnection(process, channel, remoteAddress, readThread, writeThread);
            });
            process.start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            try (var client = new ProtocolClient(process)) {
                client.handshake("localhost", ClientHandshakePacket.Intent.STATUS);
                client.send(new ClientPingRequestPacket(123));
                assertEquals(new PingResponsePacket(123), client.read());
                assertEquals(1, created.get());
            }

            server.setConnectionFactory((_, _, _, _) -> null);
            try (var client = new ProtocolClient(process)) {
                assertThrows(EOFException.class, client::read);
            }

            server.setConnectionFactory((channel, remoteAddress, readThread, writeThread) ->
                    new PlayerSocketConnection(pair.second(), channel, remoteAddress, readThread, writeThread));
            try (var client = new ProtocolClient(process)) {
                assertThrows(EOFException.class, client::read);
            }
        }
    }

    @Test
    void closingAProcessClosesSocketsWithoutPlayersAndLeavesOtherProcessesListening() throws Exception {
        try (var pair = new ServerProcessPair()) {
            pair.first().start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            pair.second().start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            try (var first = new ProtocolClient(pair.first()); var second = new ProtocolClient(pair.second())) {
                first.handshake("localhost", ClientHandshakePacket.Intent.STATUS);
                second.handshake("localhost", ClientHandshakePacket.Intent.STATUS);
                first.send(new StatusRequestPacket());
                second.send(new StatusRequestPacket());
                assertInstanceOf(ResponsePacket.class, first.read());
                assertInstanceOf(ResponsePacket.class, second.read());
                pair.first().close();
                assertThrows(EOFException.class, first::read);
                second.send(new ClientPingRequestPacket(123));
                assertEquals(new PingResponsePacket(123), second.read());
            }
        }
    }

    @Test
    void reconfigurationAcknowledgementChangesStateBeforeTheNextSocketRead() throws Exception {
        try (var pair = new ServerProcessPair();
             var server = ServerSocketChannel.open().bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
             var client = SocketChannel.open(server.getLocalAddress());
             var channel = server.accept()) {
            var process = pair.second();
            var connection = new PlayerSocketConnection(process, channel, channel.getRemoteAddress(),
                    Thread.currentThread(), Thread.currentThread());
            var player = new Player(connection, new GameProfile(UUID.randomUUID(), "Reconfigure"));
            connection.setClientState(ConnectionState.PLAY);
            connection.setServerState(ConnectionState.CONFIGURATION);
            // Isolate the incoming state transition from the asynchronous configuration response.
            process.packetListenerManager().setPlayListener(ClientConfigurationAckPacket.class, (_, _) -> {});
            var settings = settings(Locale.GERMAN);
            var buffer = NetworkBuffer.resizableBuffer(process.registries());
            try {
                PacketWriting.writeFramedPacket(buffer, ConnectionState.PLAY, new ClientConfigurationAckPacket(), 0);
                assertTrue(buffer.writeChannel(client));
                connection.read(process.packetParser());

                // Force a separate socket read before any tick can interpret queued packets.
                buffer.clear();
                PacketWriting.writeFramedPacket(buffer, ConnectionState.CONFIGURATION, new ClientSettingsPacket(settings), 0);
                assertTrue(buffer.writeChannel(client));
                connection.read(process.packetParser());
                player.interpretPacketQueue();

                assertEquals(ConnectionState.CONFIGURATION, connection.getClientState());
                assertEquals(settings, player.getSettings());
            } finally {
                connection.cleanup();
            }
        }
    }

    @Test
    void clientsJoinReconfigureAndKeepRunningWhenTheOtherProcessCloses() throws Exception {
        try (var pair = new ServerProcessPair()) {
            var first = pair.first();
            var second = pair.second();
            first.setCompressionThreshold(0);
            second.setCompressionThreshold(128);
            first.setBrandName("First");
            second.setBrandName("Second");
            first.setDifficulty(Difficulty.PEACEFUL);
            second.setDifficulty(Difficulty.HARD);
            first.commandManager().register(new Command("first"));
            second.commandManager().register(new Command("second"));
            var firstDimension = first.registries().dimensionType().register("test:shared", DimensionType.builder().ambientLight(0.1f).build());
            second.registries().dimensionType().register("test:padding", DimensionType.builder().build());
            var secondDimension = second.registries().dimensionType().register("test:shared", DimensionType.builder().ambientLight(0.9f).build());
            assertNotEquals(first.registries().dimensionType().getId(firstDimension), second.registries().dimensionType().getId(secondDimension));
            var firstInstance = first.instanceManager().createInstanceContainer(firstDimension);
            var secondInstance = second.instanceManager().createInstanceContainer(secondDimension);
            var firstEvents = new SessionEvents(first, firstInstance);
            var secondEvents = new SessionEvents(second, secondInstance);
            first.start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            second.start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));

            try (var a = new ProtocolClient(first); var b = new ProtocolClient(second)) {
                UUID firstId = UUID.randomUUID(), secondId = UUID.randomUUID();
                a.login("SameName", firstId);
                b.login("SameName", secondId);
                var firstPackets = a.finishLogin(settings(Locale.ENGLISH));
                var secondPackets = b.finishLogin(settings(Locale.FRENCH));
                Player firstPlayer = firstEvents.spawned.poll(5, TimeUnit.SECONDS);
                Player secondPlayer = secondEvents.spawned.poll(5, TimeUnit.SECONDS);
                assertNotNull(firstPlayer);
                assertNotNull(secondPlayer);
                assertSame(first, firstPlayer.process());
                assertSame(second, secondPlayer.process());
                assertEquals(Set.of(firstPlayer), first.connectionManager().getOnlinePlayers());
                assertEquals(Set.of(secondPlayer), second.connectionManager().getOnlinePlayers());
                assertEquals(firstPlayer.getEntityId(), secondPlayer.getEntityId());
                assertEquals(settings(Locale.ENGLISH), firstEvents.settings.poll(5, TimeUnit.SECONDS));
                assertEquals(Locale.ENGLISH, firstPlayer.getSettings().locale());
                assertEquals(settings(Locale.FRENCH), secondEvents.settings.poll(5, TimeUnit.SECONDS));
                assertEquals(Locale.FRENCH, secondPlayer.getSettings().locale());
                assertEquals(1, firstEvents.preLogins.get());
                assertEquals(1, secondEvents.preLogins.get());

                firstPackets.addAll(ping(a, 1));
                secondPackets.addAll(ping(b, 2));
                assertTrue(firstPackets.stream().noneMatch(SetCompressionPacket.class::isInstance));
                assertEquals(128, only(secondPackets, SetCompressionPacket.class).threshold());
                assertEquals(firstId, only(firstPackets, LoginSuccessPacket.class).gameProfile().uuid());
                assertEquals(secondId, only(secondPackets, LoginSuccessPacket.class).gameProfile().uuid());
                assertTrue(firstPackets.contains(PluginMessagePacket.brandPacket("First")));
                assertTrue(secondPackets.contains(PluginMessagePacket.brandPacket("Second")));
                assertEquals(0.1f, dimensionLight(firstPackets));
                assertEquals(0.9f, dimensionLight(secondPackets));
                assertEquals(first.registries().dimensionType().getId(firstDimension), only(firstPackets, JoinGamePacket.class).playerSpawnInfo().dimensionType());
                assertEquals(second.registries().dimensionType().getId(secondDimension), only(secondPackets, JoinGamePacket.class).playerSpawnInfo().dimensionType());
                assertEquals(Difficulty.PEACEFUL, only(firstPackets, ServerDifficultyPacket.class).difficulty());
                assertEquals(Difficulty.HARD, only(secondPackets, ServerDifficultyPacket.class).difficulty());
                assertEquals(List.of(firstId), tabList(firstPackets));
                assertEquals(List.of(secondId), tabList(secondPackets));
                assertTrue(only(firstPackets, DeclareCommandsPacket.class).nodes().stream().anyMatch(node -> node.name.equals("first")));
                assertFalse(only(firstPackets, DeclareCommandsPacket.class).nodes().stream().anyMatch(node -> node.name.equals("second")));
                assertTrue(only(secondPackets, DeclareCommandsPacket.class).nodes().stream().anyMatch(node -> node.name.equals("second")));

                a.send(new ClientPlayerLoadedPacket());
                b.send(new ClientPlayerLoadedPacket());
                assertSame(firstPlayer, firstEvents.loaded.poll(5, TimeUnit.SECONDS));
                assertSame(secondPlayer, secondEvents.loaded.poll(5, TimeUnit.SECONDS));

                // Reconfiguration stays with the same owner and preserves the player scheduler.
                secondPlayer.acquirable().sync(Player::startConfigurationPhase);
                b.readThrough(StartConfigurationPacket.class);
                b.send(new ClientConfigurationAckPacket());
                b.configure(settings(Locale.GERMAN));
                assertSame(secondPlayer, secondEvents.spawned.poll(5, TimeUnit.SECONDS));
                ping(b, 5);
                assertEquals(2, secondEvents.configurations.get());
                assertEquals(1, firstEvents.configurations.get());
                assertEquals(settings(Locale.GERMAN), secondEvents.settings.poll(5, TimeUnit.SECONDS));
                assertEquals(Locale.GERMAN, secondPlayer.getSettings().locale());
                assertDoesNotThrow(() -> secondPlayer.scheduler().scheduleNextTick(() -> {}));

                first.close();
                a.readThrough(DisconnectPacket.class);
                assertEquals(firstPlayer, firstEvents.disconnected.poll(5, TimeUnit.SECONDS));
                assertEquals(0, first.connectionManager().getOnlinePlayerCount());
                b.send(new ClientSettingsPacket(settings(Locale.ITALIAN)));
                assertEquals(settings(Locale.ITALIAN), secondEvents.settings.poll(5, TimeUnit.SECONDS));
                assertEquals(Locale.ITALIAN, secondPlayer.getSettings().locale());
                ping(b, 6);
                assertEquals(Set.of(secondPlayer), second.connectionManager().getOnlinePlayers());
                assertTrue(secondPlayer.isOnline());
                assertTrue(firstEvents.errors.isEmpty(), firstEvents.errors.toString());
                assertTrue(secondEvents.errors.isEmpty(), secondEvents.errors.toString());
            }
        }
    }

    @Test
    void packetListenersCancellationAndErrorsStayWithTheirOwner() throws Exception {
        try (var pair = new ServerProcessPair()) {
            var aEvents = new SessionEvents(pair.first(), pair.first().instanceManager().createInstanceContainer());
            var bEvents = new SessionEvents(pair.second(), pair.second().instanceManager().createInstanceContainer());
            var expected = new IllegalStateException("second listener");
            pair.first().eventHandler().addListener(PlayerPacketEvent.class, event -> {
                if (event.getPacket() instanceof ClientPingRequestPacket ping && ping.number() == 10) event.setCancelled(true);
            });
            pair.second().packetListenerManager().setPlayListener(ClientPingRequestPacket.class, (packet, player) -> {
                if (packet.number() == 20) throw expected;
                player.sendPacket(new PingResponsePacket(packet.number() + 100));
            });
            pair.first().start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            pair.second().start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            try (var a = new ProtocolClient(pair.first()); var b = new ProtocolClient(pair.second())) {
                a.login("First", UUID.randomUUID());
                b.login("Second", UUID.randomUUID());
                a.finishLogin(ClientSettings.DEFAULT);
                b.finishLogin(ClientSettings.DEFAULT);
                assertNotNull(aEvents.spawned.poll(5, TimeUnit.SECONDS));
                assertNotNull(bEvents.spawned.poll(5, TimeUnit.SECONDS));
                a.send(new ClientPingRequestPacket(10));
                assertEquals(List.of(11L), ping(a, 11).stream().filter(PingResponsePacket.class::isInstance)
                        .map(PingResponsePacket.class::cast).map(PingResponsePacket::number).toList());
                b.send(new ClientPingRequestPacket(20));
                assertSame(expected, bEvents.errors.poll(5, TimeUnit.SECONDS));
                assertTrue(aEvents.errors.isEmpty());
                b.send(new ClientPingRequestPacket(21));
                assertEquals(121, only(b.readThrough(PingResponsePacket.class), PingResponsePacket.class).number());
            }
        }
    }

    @Test
    void backendReconnectCreatesADestinationPlayerRepresentation() throws Exception {
        try (var pair = new ServerProcessPair()) {
            var firstEvents = new SessionEvents(pair.first(), pair.first().instanceManager().createInstanceContainer());
            var secondEvents = new SessionEvents(pair.second(), pair.second().instanceManager().createInstanceContainer());
            var firstTransfers = new AtomicInteger();
            var secondTransfers = new AtomicInteger();
            pair.first().eventHandler().addListener(OutgoingTransferEvent.class, _ -> firstTransfers.incrementAndGet());
            pair.second().eventHandler().addListener(OutgoingTransferEvent.class, _ -> secondTransfers.incrementAndGet());
            pair.first().start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            pair.second().start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            UUID uuid = UUID.randomUUID();
            Player source;
            try (var client = new ProtocolClient(pair.first())) {
                client.login("Transferred", uuid);
                client.finishLogin(settings(Locale.ENGLISH));
                source = firstEvents.spawned.poll(5, TimeUnit.SECONDS);
                assertNotNull(source);
                source.getPlayerConnection().transfer("localhost", pair.second().server().getPort());
                var transfer = only(client.readThrough(TransferPacket.class), TransferPacket.class);
                assertEquals(pair.second().server().getPort(), transfer.port());
            }
            assertSame(source, firstEvents.disconnected.poll(5, TimeUnit.SECONDS));
            // A proxy reconnect uses a new backend connection with the same authenticated identity.
            try (var client = new ProtocolClient(pair.second())) {
                client.login("Transferred", uuid);
                client.finishLogin(settings(Locale.FRENCH));
                var destination = secondEvents.spawned.poll(5, TimeUnit.SECONDS);
                assertNotNull(destination);
                assertNotSame(source, destination);
                assertEquals(source.getUuid(), destination.getUuid());
                assertSame(pair.second(), destination.process());
                assertSame(pair.first(), source.process());
                assertEquals(0, pair.first().connectionManager().getOnlinePlayerCount());
                assertEquals(Set.of(destination), pair.second().connectionManager().getOnlinePlayers());
                assertEquals(1, firstTransfers.get());
                assertEquals(0, secondTransfers.get());
                ping(client, 1);
                assertTrue(firstEvents.errors.isEmpty(), firstEvents.errors.toString());
                assertTrue(secondEvents.errors.isEmpty(), secondEvents.errors.toString());
            }
        }
    }

    private static ClientSettings settings(Locale locale) {
        var defaults = ClientSettings.DEFAULT;
        return new ClientSettings(locale, (byte) 2, defaults.chatMessageType(), defaults.chatColors(),
                defaults.displayedSkinParts(), defaults.mainHand(), defaults.enableTextFiltering(),
                defaults.allowServerListings(), defaults.particleSetting());
    }

    // This immediate read-thread response does not wait for queued tick-thread handlers.
    private static List<ServerPacket> ping(ProtocolClient client, long id) throws Exception {
        client.send(new ClientPingRequestPacket(id));
        var packets = client.readThrough(PingResponsePacket.class);
        assertEquals(id, only(packets, PingResponsePacket.class).number());
        return packets;
    }

    private static float dimensionLight(List<ServerPacket> packets) {
        var dimension = packets.stream().filter(RegistryDataPacket.class::isInstance).map(RegistryDataPacket.class::cast)
                .filter(packet -> packet.registryId().equals("minecraft:dimension_type")).findFirst().orElseThrow();
        var data = dimension.entries().stream().filter(entry -> entry.id().equals("test:shared")).findFirst().orElseThrow().data();
        return assertInstanceOf(CompoundBinaryTag.class, data).getFloat("ambient_light");
    }

    private static List<UUID> tabList(List<ServerPacket> packets) {
        return packets.stream().filter(PlayerInfoUpdatePacket.class::isInstance).map(PlayerInfoUpdatePacket.class::cast)
                .filter(packet -> packet.actions().contains(PlayerInfoUpdatePacket.Action.ADD_PLAYER))
                .flatMap(packet -> packet.entries().stream()).map(PlayerInfoUpdatePacket.Entry::uuid).toList();
    }

    private static <T> T only(List<ServerPacket> packets, Class<T> type) {
        var matches = packets.stream().filter(type::isInstance).map(type::cast).toList();
        assertEquals(1, matches.size(), type.getSimpleName());
        return matches.getFirst();
    }

    private static final class SessionEvents {
        final LinkedBlockingQueue<Player> spawned = new LinkedBlockingQueue<>();
        final LinkedBlockingQueue<Player> disconnected = new LinkedBlockingQueue<>();
        final LinkedBlockingQueue<Throwable> errors = new LinkedBlockingQueue<>();
        final AtomicInteger preLogins = new AtomicInteger();
        final AtomicInteger configurations = new AtomicInteger();
        final LinkedBlockingQueue<Player> loaded = new LinkedBlockingQueue<>();
        final LinkedBlockingQueue<ClientSettings> settings = new LinkedBlockingQueue<>();

        SessionEvents(ServerProcess process, Instance instance) {
            process.exceptionManager().setExceptionHandler(errors::add);
            process.connectionManager().setPlayerProvider((connection, profile) -> {
                assertSame(process, connection.process());
                return new Player(connection, profile);
            });
            process.eventHandler().addListener(AsyncPlayerPreLoginEvent.class, (owner, event) -> {
                assertSame(process, owner);
                assertSame(owner, event.getConnection().process());
                preLogins.incrementAndGet();
            });
            process.eventHandler().addListener(AsyncPlayerConfigurationEvent.class, event -> {
                assertSame(process, event.getPlayer().process());
                event.setSpawningInstance(instance);
                event.getPlayer().setRespawnPoint(new Pos(0, 40, 0));
                configurations.incrementAndGet();
            });
            process.eventHandler().addListener(PlayerSettingsChangeEvent.class, event -> {
                assertSame(process, event.getPlayer().process());
                settings.add(event.getPlayer().getSettings());
            });
            process.eventHandler().addListener(PlayerLoadedEvent.class, event -> loaded.add(event.getPlayer()));
            process.eventHandler().addListener(PlayerSpawnEvent.class, event -> spawned.add(event.getPlayer()));
            process.eventHandler().addListener(PlayerDisconnectEvent.class, event -> disconnected.add(event.getPlayer()));
        }
    }
}

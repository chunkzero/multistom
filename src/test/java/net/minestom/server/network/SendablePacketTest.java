package net.minestom.server.network;

import net.kyori.adventure.text.Component;
import net.minestom.server.entity.PlayerHand;
import net.minestom.server.network.packet.PacketBufferPool;
import net.minestom.server.network.packet.PacketEncodingContext;
import net.minestom.server.network.packet.PacketReading;
import net.minestom.server.network.packet.PacketVanilla;
import net.minestom.server.network.packet.PacketWriting;
import net.minestom.server.network.packet.client.ClientPacket;
import net.minestom.server.network.packet.client.play.ClientAnimationPacket;
import net.minestom.server.network.packet.server.CachedPacket;
import net.minestom.server.network.packet.server.play.SystemChatPacket;
import net.minestom.server.registry.Registries;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.DataFormatException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class SendablePacketTest {

    @Test
    public void cached() {
        try (var pool = new PacketBufferPool(Registries.vanilla())) {
            var context = pool.context(ConnectionState.PLAY, 256);
            var packet = new SystemChatPacket(Component.text("Hello World!"), false);
            var cached = new CachedPacket(packet);
            assertSame(packet, cached.packet(context));
            var buffer = context.frame(packet);
            var cachedBuffer = cached.body(context);
            assertTrue(NetworkBuffer.equals(buffer, cachedBuffer));
            assertSame(cached.body(context), cachedBuffer);
            assertSame(packet, cached.packet(context));
        }
    }

    @Test
    public void cachedBodyReusesCachedPacket() {
        try (var pool = new PacketBufferPool(Registries.vanilla())) {
            var context = pool.context(ConnectionState.PLAY, 256);
            var packet = new SystemChatPacket(Component.text("Hello World!"), false);
            var calls = new AtomicInteger();
            var cached = new CachedPacket(() -> {
                calls.incrementAndGet();
                return packet;
            });
            assertFalse(cached.isValid());

            // May fail in the very unlikely case where soft references are cleared
            assertSame(packet, cached.packet(context));
            assertSame(packet, cached.packet(context));
            assertEquals(1, calls.get());
            assertTrue(cached.isValid());

            var buffer = context.frame(packet);
            var cachedBuffer = cached.body(context);
            assertTrue(NetworkBuffer.equals(buffer, cachedBuffer));
            assertSame(cachedBuffer, cached.body(context));
            assertSame(packet, cached.packet(context));
            assertEquals(1, calls.get());

            cached.invalidate();
            assertFalse(cached.isValid());
            assertTrue(NetworkBuffer.equals(buffer, cached.body(context)));
            assertSame(packet, cached.packet(context));
            assertEquals(2, calls.get());
        }
    }

    @Test
    public void cachedPacketIsNotFramed() {
        try (var pool = new PacketBufferPool(Registries.vanilla())) {
            // System chat has no packet ID in the login state, so framing it there fails
            var packet = new SystemChatPacket(Component.text("Hello World!"), false);
            var cached = new CachedPacket(packet);
            assertSame(packet, cached.packet(pool.context(ConnectionState.LOGIN, 256)));
            assertTrue(cached.isValid());
            var context = pool.context(ConnectionState.PLAY, 256);
            assertTrue(NetworkBuffer.equals(context.frame(packet), cached.body(context)));
        }
    }

    @Test
    @Timeout(10)
    void concurrentContextsSerializeTheSupplier() throws Exception {
        try (var pool = new PacketBufferPool(Registries.vanilla());
             var executor = Executors.newFixedThreadPool(2)) {
            var packet = new SystemChatPacket(Component.text("shared"), false);
            var active = new AtomicInteger();
            var calls = new AtomicInteger();
            var cached = new CachedPacket(() -> {
                assertEquals(1, active.incrementAndGet());
                try {
                    calls.incrementAndGet();
                    return packet;
                } finally {
                    active.decrementAndGet();
                }
            });
            var barrier = new CyclicBarrier(2);
            var firstContext = pool.context(ConnectionState.PLAY, 0);
            var secondContext = pool.context(ConnectionState.PLAY, 1);
            var first = executor.submit(() -> readConcurrentCache(cached, firstContext, barrier));
            var second = executor.submit(() -> readConcurrentCache(cached, secondContext, barrier));
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            // May fail in the very unlikely case where soft references are cleared
            assertEquals(1, calls.get());
            cached.invalidate();
            assertFalse(cached.isValid());
            assertSame(packet, cached.packet(firstContext));
            assertEquals(2, calls.get());
        }
    }

    private static Void readConcurrentCache(CachedPacket cached, PacketEncodingContext context, CyclicBarrier barrier)
            throws Exception {
        var expected = context.frame(new SystemChatPacket(Component.text("shared"), false));
        for (int i = 0; i < 16; i++) {
            barrier.await(5, TimeUnit.SECONDS);
            assertTrue(NetworkBuffer.equals(expected, cached.body(context)));
        }
        return null;
    }

    @Test
    @Timeout(10)
    void invalidationDoesNotWaitForAnInProgressSupplier() throws Exception {
        try (var pool = new PacketBufferPool(Registries.vanilla());
             var executor = Executors.newFixedThreadPool(2)) {
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var cached = new CachedPacket(() -> {
                entered.countDown();
                try {
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                return new SystemChatPacket(Component.text("pending"), false);
            });
            var future = executor.submit(() -> cached.body(pool.context(ConnectionState.PLAY, 0)));
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                executor.submit(cached::invalidate).get(5, TimeUnit.SECONDS);
                assertFalse(cached.isValid());
            } finally {
                release.countDown();
            }
            future.get(5, TimeUnit.SECONDS);
            assertTrue(cached.isValid(), "An admitted computation can publish after invalidation");
        }
    }

    @Test
    public void trimmed() throws DataFormatException {
        var packet = new ClientAnimationPacket(PlayerHand.MAIN);

        var buffer = PacketWriting.allocateTrimmedPacket(NetworkBuffer.staticBuffer(16),
                PacketVanilla.CLIENT_PACKET_PARSER, ConnectionState.PLAY, packet, 0);

        var result = PacketReading.readClient(buffer, ConnectionState.PLAY, false);
        if (!(result instanceof PacketReading.Result.Success<ClientPacket>(
                List<PacketReading.ParsedPacket<ClientPacket>> packets
        ))) {
            fail();
            return;
        }
        assertEquals(1, packets.size());
        ClientPacket readPacket = packets.getFirst().packet();
        assertEquals(packet, readPacket);
    }
}

package net.minestom.server.network.packet.server;

import net.minestom.server.network.NetworkBuffer;
import net.minestom.server.network.packet.PacketEncodingContext;
import net.minestom.server.property.ServerProperties;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.SoftReference;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Represents a packet whose framed representation is computed lazily and
 * cached for reuse.
 *
 * <p>The cached value is stored in a {@link SoftReference}, allowing it to be
 * reclaimed in response to memory pressure. The value is recomputed when the
 * reference has been cleared or when {@link #invalidate()} is called.
 *
 * <p>When caching is enabled, cache computation is serialized so that only one
 * thread invokes the packet supplier and computes a replacement value at a
 * time. Cache reads and publication use acquire and release memory semantics.
 *
 * <p>When caching is disabled, calls to {@link #packet(PacketEncodingContext)} invoke
 * the packet supplier directly. The supplier must therefore be thread safe if
 * this object may be accessed concurrently while caching is disabled.
 *
 * <p>One framed representation is retained, for the most recently used encoding context.
 * Sharing this object across owners, connection states, or compression settings is safe:
 * a different context replaces the framed representation and reuses the cached packet. Registry definitions must
 * remain stable while the representation is cached; call {@link #invalidate()} after
 * changing definitions or the supplied packet.
 */
@ApiStatus.Internal
public final class CachedPacket implements SendablePacket {
    private static final VarHandle PACKET;

    static {
        try {
            PACKET = MethodHandles.lookup().findVarHandle(CachedPacket.class, "cache", SoftReference.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final Supplier<ServerPacket> packetSupplier;

    // Accessed through PACKET using acquire/release semantics.
    @SuppressWarnings("unused")
    private @Nullable SoftReference<Cache> cache;

    /**
     * Creates a cached packet backed by the given packet supplier.
     *
     * <p>The supplier is invoked whenever the framed packet must be recomputed.
     * It may therefore be invoked multiple times over the lifetime of this
     * object.
     *
     * <p>When caching is enabled, supplier invocation is serialized. When
     * caching is disabled, the supplier may be invoked concurrently and must be
     * thread safe if this object is accessed by multiple threads.
     *
     * @param packetSupplier the packet supplier
     * @throws NullPointerException if {@code packetSupplier} is {@code null}
     */
    public CachedPacket(Supplier<ServerPacket> packetSupplier) {
        this.packetSupplier = Objects.requireNonNull(packetSupplier, "packetSupplier");
    }

    /**
     * Creates a cached packet backed by a constant packet value.
     *
     * @param packet the packet to frame and cache
     * @throws NullPointerException if {@code packet} is {@code null}
     */
    public CachedPacket(ServerPacket packet) {
        Objects.requireNonNull(packet, "packet");
        this(() -> packet);
    }

    /**
     * Invalidates the currently cached packet, if any.
     *
     * <p>The next cache access recomputes the packet. The reference is cleared even while
     * caching is disabled, so a retained value cannot resurface once caching is re-enabled.
     *
     * @implSpec This method clears the cached reference using release semantics.
     * Cache computation is not synchronized with invalidation, so an in progress
     * computation may publish a value after this method returns.
     */
    public void invalidate() {
        PACKET.setRelease(this, null);
    }

    /**
     * Returns the packet represented by this cached value.
     *
     * <p>When caching is enabled, this method may initialize or reuse the cached
     * packet. Supplier invocation and cache computation are serialized.
     *
     * <p>When caching is disabled, the packet supplier is invoked directly and
     * may be invoked concurrently by multiple callers.
     *
     * @param context the owner, connection state, and negotiated compression of the caller
     * @return the packet value
     */
    public ServerPacket packet(PacketEncodingContext context) {
        final ServerProperties properties = context.properties();
        if (!properties.cachedPacket().get()) return packetSupplier.get();
        final Cache cache = cache();
        return cache != null ? cache.packet() : computePacket(properties);
    }

    /**
     * Returns the cached framed packet body for the given encoding context.
     *
     * <p>If caching is enabled, this method frames the packet for
     * {@code context} when no body is cached for it yet, reusing the cached
     * packet when one exists. Cache computation and supplier invocation are
     * serialized.
     *
     * <p>If caching is disabled, no framed body is created and {@code null} is
     * returned.
     *
     * @param context the owner, connection state, and negotiated compression used when framing
     * @return the framed packet body, or {@code null} when caching is disabled
     */
    public @Nullable NetworkBuffer body(PacketEncodingContext context) {
        final FramedPacket framed = framed(context);
        return framed != null ? framed.body() : null;
    }

    /**
     * Returns the framed packet cached for the given encoding context, computing it when absent.
     *
     * @param context the owner, connection state, and negotiated compression used when framing
     * @return the cached framed packet, or {@code null} when caching is disabled
     */
    public @Nullable FramedPacket framed(PacketEncodingContext context) {
        if (!context.properties().cachedPacket().get()) return null;

        final Cache cache = cache();
        final FramedPacket framed = cache != null ? cache.framed() : null;
        return framed != null && framed.context().equals(context) ? framed : computeFramed(context);
    }

    /**
     * Returns the cached packet, computing it when absent.
     *
     * <p>This method is synchronized so that only one thread invokes the packet
     * supplier at a time. The cache is checked again after acquiring this
     * object's monitor because another thread may have populated it while the
     * current thread was waiting.
     *
     * @param properties the properties of the owner requesting the packet
     * @return the existing or newly computed packet
     */
    private synchronized ServerPacket computePacket(ServerProperties properties) {
        final Cache cache = cache();
        if (cache != null) return cache.packet();
        final ServerPacket packet = packetSupplier.get();
        PACKET.setRelease(this, new SoftReference<>(new Cache(properties, packet, null)));
        return packet;
    }

    /**
     * Returns the existing framed packet or computes and publishes one.
     *
     * <p>This method is synchronized so that only one thread invokes the packet
     * supplier and frames a replacement at a time. The cache is checked again
     * after acquiring this object's monitor because another thread may have
     * populated it while the current thread was waiting.
     *
     * <p>Invalidation does not acquire this monitor. An invalidation occurring
     * during computation may therefore be followed by publication of the
     * in progress result.
     *
     * @param context the owner, connection state, and negotiated compression used when framing
     * @return the existing or newly framed packet
     */
    private synchronized FramedPacket computeFramed(PacketEncodingContext context) {
        final Cache cache = cache();
        final FramedPacket cached = cache != null ? cache.framed() : null;
        if (cached != null && cached.context().equals(context)) return cached;

        final ServerPacket packet = cache != null ? cache.packet() : packetSupplier.get();
        final FramedPacket framed = new FramedPacket(context, packet, context.frame(packet));
        PACKET.setRelease(this, new SoftReference<>(new Cache(context.properties(), packet, framed)));
        return framed;
    }

    /**
     * Returns the currently referenced cache.
     *
     * <p>The cached reference is read using acquire semantics.
     *
     * @return the cache, or {@code null} if no cache exists or the soft
     * reference has been cleared
     */
    @SuppressWarnings("unchecked")
    private @Nullable Cache cache() {
        final SoftReference<Cache> ref = (SoftReference<Cache>) PACKET.getAcquire(this);
        return ref != null ? ref.get() : null;
    }

    /**
     * Returns whether a packet is currently cached and has not been
     * reclaimed. The cached packet may not have been framed yet.
     *
     * <p>This result is inherently transient because the underlying soft
     * reference may be cleared or the cache may be invalidated immediately
     * after this method returns.
     *
     * @return {@code true} if caching is enabled and a cached value is currently
     * available
     */
    public boolean isValid() {
        final Cache cache = cache();
        return cache != null && cache.properties().cachedPacket().get();
    }

    /**
     * Returns a string representation containing the currently referenced
     * cached value.
     *
     * <p>Calling this method does not invoke the packet supplier or repopulate
     * the cache.
     *
     * @return a string representation of this cached packet
     */
    @Override
    public String toString() {
        return String.format("CachedPacket{cache=%s}", cache());
    }

    // The packet, plus its framing for the most recently used encoding context once one requested it.
    // Requires identity
    private record Cache(ServerProperties properties, ServerPacket packet, @Nullable FramedPacket framed) {
    }
}

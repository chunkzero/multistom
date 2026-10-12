package net.minestom.server.adventure.serializer.nbt;

import net.kyori.adventure.nbt.BinaryTag;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.serializer.ComponentSerializer;
import net.minestom.server.registry.Registries;

import java.util.Objects;

public sealed interface NbtComponentSerializer extends ComponentSerializer<Component, Component, BinaryTag> permits NbtComponentSerializerImpl {
    /**
     * Returns a serializer bound to the given registry context.
     *
     * @param registries the registry context used to resolve registry references
     * @return the serializer
     */
    static NbtComponentSerializer nbt(Registries registries) {
        Objects.requireNonNull(registries, "registries");
        return new NbtComponentSerializerImpl(registries);
    }
}

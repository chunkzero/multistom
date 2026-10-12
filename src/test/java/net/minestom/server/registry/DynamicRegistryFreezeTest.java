package net.minestom.server.registry;

import net.kyori.adventure.key.Key;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DynamicRegistryFreezeTest {
    @Test
    void explicitFreezeRejectsEntryChangesAndLeavesTagsMutable() {
        var registry = DynamicRegistry.fromMap(Key.key("test:registry"), Map.entry(Key.key("test:entry"), "original"));
        var entryKey = registry.getKey(Key.key("test:entry"));
        var tagKey = TagKey.<String>of("test:tag");
        assertFalse(registry.isFrozen());
        registry.freeze();
        registry.freeze();
        assertTrue(registry.isFrozen());
        assertThrows(UnsupportedOperationException.class, () -> registry.register("test:new", "new"));
        assertThrows(UnsupportedOperationException.class, () -> registry.register("test:entry", "replacement"));
        assertThrows(UnsupportedOperationException.class, () -> registry.remove(Key.key("test:entry")));
        assertEquals(1, registry.size());
        assertEquals("original", registry.get(entryKey));
        assertEquals(0, registry.getId(entryKey));
        long revision = registry.tagsRevision();
        assertEquals(tagKey, registry.getOrCreateTag(tagKey).key());
        assertTrue(registry.tagsRevision() > revision);
        assertTrue(registry.removeTag(tagKey));
    }
}

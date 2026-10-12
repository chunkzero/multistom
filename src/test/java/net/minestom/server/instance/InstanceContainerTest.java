package net.minestom.server.instance;

import net.kyori.adventure.key.Key;
import net.minestom.server.ServerProcess;
import net.minestom.server.registry.Registries;
import net.minestom.server.tag.Tag;
import net.minestom.server.world.DimensionType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class InstanceContainerTest {
    private final ServerProcess process = ServerProcess.create();

    @AfterAll
    void closeProcess() {
        process.close();
    }

    @Test
    public void copyPreservesTag() {
        var tag = Tag.String("test");
        var instance = new InstanceContainer(process, UUID.randomUUID(), DimensionType.OVERWORLD);
        instance.setTag(tag, "123");

        var copyInstance = instance.copy();
        var result = copyInstance.getTag(tag);
        assertEquals("123", result);
    }

    @Test
    public void derivedInstancesPreserveProcessAndDimensionName() {
        final Key dimensionName = Key.key("minestom:derived");
        final InstanceContainer instance = new InstanceContainer(
                process, UUID.randomUUID(),
                DimensionType.OVERWORLD, null, dimensionName);

        final InstanceContainer copy = instance.copy();
        final SharedInstance shared = new SharedInstance(UUID.randomUUID(), instance);

        assertSame(process, copy.process());
        assertSame(process, shared.process());
        assertSame(process.registries(), copy.registries());
        assertSame(process.registries(), shared.registries());
        assertEquals(dimensionName.asString(), copy.getDimensionName());
        assertEquals(dimensionName.asString(), shared.getDimensionName());
    }

    @Test
    public void explicitRegistriesAreUsedAndPreserved() {
        final Key dimensionName = Key.key("minestom:explicit");
        final Registries registries = Registries.vanilla();
        final var dimension = registries.dimensionType().register("test:explicit", DimensionType.builder().ambientLight(0.7f).build());

        assertThrows(IllegalArgumentException.class, () -> new InstanceContainer(process, UUID.randomUUID(), dimension));

        final InstanceContainer instance = new InstanceContainer(process, registries, UUID.randomUUID(), dimension, null, dimensionName);
        final InstanceContainer copy = instance.copy();
        final SharedInstance shared = new SharedInstance(UUID.randomUUID(), instance);
        final InstanceContainer managed = process.instanceManager().createInstanceContainer(registries, dimension);

        for (Instance derived : new Instance[]{instance, copy, shared, managed}) {
            assertSame(process, derived.process());
            assertSame(registries, derived.registries());
            assertSame(registries.dimensionType().get(dimension), derived.getCachedDimensionType());
        }
        assertSame(registries, instance.loadChunk(0, 0).join().tagRegistries());
        assertEquals(dimensionName.asString(), copy.getDimensionName());
        assertEquals(dimensionName.asString(), shared.getDimensionName());
        assertTrue(process.instanceManager().getInstances().contains(managed));
    }
}

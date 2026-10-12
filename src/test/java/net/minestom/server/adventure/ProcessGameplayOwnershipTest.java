package net.minestom.server.adventure;

import net.kyori.adventure.bossbar.BossBar;
import net.kyori.adventure.key.Key;
import net.kyori.adventure.sound.Sound;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.event.ClickCallback;
import net.kyori.adventure.text.event.ClickEvent;
import net.minestom.server.advancements.AdvancementRoot;
import net.minestom.server.advancements.AdvancementTab;
import net.minestom.server.advancements.FrameType;
import net.minestom.server.adventure.audience.PacketGroupingAudience;
import net.minestom.server.command.builder.Command;
import net.minestom.server.command.builder.CommandContext;
import net.minestom.server.command.builder.arguments.ArgumentType;
import net.minestom.server.coordinate.Pos;
import net.minestom.server.entity.damage.Damage;
import net.minestom.server.entity.damage.DamageType;
import net.minestom.server.event.inventory.InventoryItemChangeEvent;
import net.minestom.server.event.player.PlayerCommandEvent;
import net.minestom.server.inventory.Inventory;
import net.minestom.server.inventory.InventoryType;
import net.minestom.server.inventory.type.EnchantmentTableInventory;
import net.minestom.server.item.ItemStack;
import net.minestom.server.item.Material;
import net.minestom.server.item.enchant.Enchantment;
import net.minestom.server.network.packet.client.common.ClientCustomClickActionPacket;
import net.minestom.server.network.packet.server.play.ActionBarPacket;
import net.minestom.server.network.packet.server.play.BossBarPacket;
import net.minestom.server.network.packet.server.play.DamageEventPacket;
import net.minestom.server.network.packet.server.play.EntitySoundEffectPacket;
import net.minestom.server.network.packet.server.play.TeamsPacket;
import net.minestom.server.network.packet.server.play.WindowPropertyPacket;
import net.minestom.server.network.player.GameProfile;
import net.minestom.server.sound.SoundEvent;
import net.minestom.server.utils.PacketSendingUtils;
import net.minestom.server.utils.entity.EntityFinder;
import net.minestom.testing.Env;
import net.minestom.testing.ServerProcessPair;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcessGameplayOwnershipTest {
    @Test
    void bossBarPacketsAreGroupedWithinEachProcess() {
        try (var pair = new ServerProcessPair(); var a = Env.create(pair.first()); var b = Env.create(pair.second())) {
            var ac = a.createConnection();
            var ac2 = a.createConnection();
            var bc = b.createConnection();
            var bc2 = b.createConnection();
            var ai = a.createEmptyInstance();
            var bi = b.createEmptyInstance();
            var first = ac.connect(ai, Pos.ZERO);
            var firstOther = ac2.connect(ai, Pos.ZERO);
            var second = bc.connect(bi, Pos.ZERO);
            var secondOther = bc2.connect(bi, Pos.ZERO);
            var packetsA = ac.trackIncoming(BossBarPacket.class);
            var packetsA2 = ac2.trackIncoming(BossBarPacket.class);
            var packetsB = bc.trackIncoming(BossBarPacket.class);
            var packetsB2 = bc2.trackIncoming(BossBarPacket.class);
            var bar = BossBar.bossBar(Component.text("grouped"), 1, BossBar.Color.BLUE, BossBar.Overlay.PROGRESS);
            var audience = PacketGroupingAudience.of(List.of(first, second, firstOther, secondOther));
            audience.showBossBar(bar);
            audience.hideBossBar(bar);
            var firstPackets = packetsA.collect();
            var firstOtherPackets = packetsA2.collect();
            var secondPackets = packetsB.collect();
            var secondOtherPackets = packetsB2.collect();
            assertEquals(2, firstPackets.size());
            assertEquals(2, firstOtherPackets.size());
            assertEquals(2, secondPackets.size());
            assertEquals(2, secondOtherPackets.size());
            for (int i = 0; i < 2; i++) {
                assertSame(firstPackets.get(i), firstOtherPackets.get(i));
                assertSame(secondPackets.get(i), secondOtherPackets.get(i));
                assertNotEquals(firstPackets.get(i).uuid(), secondPackets.get(i).uuid());
            }
            assertTrue(a.process().bossBarManager().getBossBarViewers(bar).isEmpty());
            assertTrue(b.process().bossBarManager().getBossBarViewers(bar).isEmpty());
        }
    }

    @Test
    void teamExistenceUsesNamesWithinTheOwningProcess() {
        try (var pair = new ServerProcessPair()) {
            var first = pair.first().teamManager();
            var second = pair.second().teamManager();
            var original = first.createTeam("same");
            var foreign = second.createTeam("same");
            assertTrue(first.exists(original));
            assertTrue(second.exists(foreign));
            assertFalse(first.exists(foreign));
            assertFalse(second.exists(original));

            assertTrue(first.deleteTeam(original));
            assertFalse(first.exists(original));
            var replacement = first.createTeam("same");
            assertNotSame(original, replacement);
            assertTrue(first.exists(original));
            assertSame(replacement, first.getTeam("same"));
        }
    }

    @Test
    void buildersCannotRegisterDuplicateTeamNames() {
        try (var pair = new ServerProcessPair(); var env = Env.create(pair.first())) {
            var connection = env.createConnection();
            connection.connect(env.createEmptyInstance(), Pos.ZERO);
            var packets = connection.trackIncoming(TeamsPacket.class);
            var manager = env.process().teamManager();
            var first = manager.createBuilder("same");
            var second = manager.createBuilder("same");
            var team = first.build();
            assertThrows(IllegalArgumentException.class, second::build);
            assertSame(team, manager.getTeam("same"));
            assertSame(team, first.build());
            assertSame(team, manager.createTeam("same"));
            assertEquals(Set.of(team), manager.getTeams());
            packets.assertSingle();
            assertTrue(pair.second().teamManager().getTeams().isEmpty());
        }
    }

    @Test
    void commandEventsVisibilitySelectorsAndBroadcastsUseTheOwner() {
        try (var pair = new ServerProcessPair(); var a = Env.create(pair.first()); var b = Env.create(pair.second())) {
            var profile = new GameProfile(UUID.randomUUID(), "SameName");
            var firstConnection = a.createConnection(profile);
            var secondConnection = b.createConnection(profile);
            var first = firstConnection.connect(a.createEmptyInstance(), Pos.ZERO);
            var second = secondConnection.connect(b.createEmptyInstance(), Pos.ZERO);
            var firstEvents = new AtomicInteger();
            var secondEvents = new AtomicInteger();
            a.process().eventHandler().addListener(PlayerCommandEvent.class, event -> {
                firstEvents.incrementAndGet();
                event.setCommand("same");
            });
            b.process().eventHandler().addListener(PlayerCommandEvent.class, event -> {
                secondEvents.incrementAndGet();
                event.setCommand("same");
            });
            var target = ArgumentType.Entity("targets").setDefaultValue((_, context) ->
                    new EntityFinder(context.process()).setTargetSelector(EntityFinder.TargetSelector.ALL_PLAYERS));
            var command = new Command("same");
            command.addSyntax((sender, context) -> {
                assertEquals(List.of(context.process() == a.process() ? first : second), context.get(target).find(sender));
                context.process().audiences().players().sendActionBar(Component.text(context.process() == a.process() ? "first" : "second"));
            }, target);
            a.process().commandManager().register(command);
            b.process().commandManager().register(command);
            var firstPackets = firstConnection.trackIncoming(ActionBarPacket.class);
            var secondPackets = secondConnection.trackIncoming(ActionBarPacket.class);
            a.process().commandManager().execute(first, "rewrite");
            b.process().commandManager().execute(second, "rewrite");
            firstPackets.assertSingle(packet -> assertEquals(Component.text("first"), packet.text()));
            secondPackets.assertSingle(packet -> assertEquals(Component.text("second"), packet.text()));
            assertEquals(1, firstEvents.get());
            assertEquals(1, secondEvents.get());
            assertThrows(IllegalArgumentException.class, () -> a.process().commandManager().execute(second, "same"));
            assertThrows(IllegalArgumentException.class, () -> a.process().commandManager().parseCommand(second, "same"));
            var parsed = a.process().commandManager().parseCommand(first, "same");
            assertThrows(IllegalArgumentException.class, () -> parsed.executable().execute(second));
            assertEquals(1, firstEvents.get());
            assertEquals(1, secondEvents.get());

            var visible = new Command("visible");
            visible.setCondition((_, context) -> {
                assertEquals("", context.getInput());
                assertEquals(CommandContext.Purpose.DECLARATION, context.purpose());
                assertFalse(context.has("declaration-state"));
                context.setArg("declaration-state", true, "");
                return context.process() == b.process();
            });
            var otherVisible = new Command("visible-other");
            otherVisible.setCondition(visible.getCondition());
            a.process().commandManager().register(visible, otherVisible);
            b.process().commandManager().register(visible, otherVisible);
            assertFalse(a.process().commandManager().createDeclareCommandsPacket(first).nodes().stream().anyMatch(node -> "visible".equals(node.name)));
            assertTrue(b.process().commandManager().createDeclareCommandsPacket(second).nodes().stream().anyMatch(node -> "visible".equals(node.name)));
            var finder = new EntityFinder(a.process()).setTargetSelector(EntityFinder.TargetSelector.ALL_PLAYERS);
            assertEquals(List.of(first), finder.find(a.process().commandManager().getConsoleSender()));
            assertEquals(List.of(first), finder.find(first));
            assertSame(first, finder.findFirstPlayer(first));
            assertSame(first, finder.findFirstEntity(first));
            var namedFinder = new EntityFinder(b.process()).setTargetSelector(EntityFinder.TargetSelector.MINESTOM_USERNAME).setConstantName("SameName");
            assertEquals(List.of(second), namedFinder.find(b.process().commandManager().getConsoleSender()));
            assertThrows(IllegalArgumentException.class, () -> finder.find(second));

            var aOnly = firstConnection.trackIncoming(ActionBarPacket.class);
            var bNone = secondConnection.trackIncoming(ActionBarPacket.class);
            PacketSendingUtils.broadcastPlayPacket(a.process(), new ActionBarPacket(Component.text("owner")));
            aOnly.assertSingle();
            bNone.assertEmpty();
        }
    }

    @Test
    void sharedBossBarsAndTeamNamesStayIndependentWhenOneProcessCloses() {
        try (var pair = new ServerProcessPair(); var a = Env.create(pair.first()); var b = Env.create(pair.second())) {
            var profile = new GameProfile(UUID.randomUUID(), "SameName");
            var ac = a.createConnection(profile);
            var bc = b.createConnection(profile);
            var first = ac.connect(a.createEmptyInstance(), Pos.ZERO);
            var second = bc.connect(b.createEmptyInstance(), Pos.ZERO);
            var sound = Sound.sound(SoundEvent.BLOCK_NOTE_BLOCK_BELL, Sound.Source.MASTER, 1, 1);
            var aSounds = ac.trackIncoming(EntitySoundEffectPacket.class);
            var bSounds = bc.trackIncoming(EntitySoundEffectPacket.class);
            assertThrows(IllegalArgumentException.class, () -> second.playSound(sound, first));
            assertThrows(IllegalArgumentException.class, () -> PacketGroupingAudience.of(List.of(first, second)).playSound(sound, first));
            aSounds.assertEmpty();
            bSounds.assertEmpty();
            var tabA = a.process().advancementManager().createTab("test:shared", new AdvancementRoot(Component.text("A"), Component.empty(), Material.DIAMOND, FrameType.TASK, 0, 0, null));
            var tabB = b.process().advancementManager().createTab("test:shared", new AdvancementRoot(Component.text("B"), Component.empty(), Material.DIAMOND, FrameType.TASK, 0, 0, null));
            tabA.addViewer(first);
            tabB.addViewer(second);
            assertEquals(Set.of(tabA), AdvancementTab.getTabs(first));
            assertEquals(Set.of(tabB), AdvancementTab.getTabs(second));
            assertThrows(IllegalArgumentException.class, () -> tabA.addViewer(second));
            var aTeams = ac.trackIncoming(TeamsPacket.class);
            var bTeams = bc.trackIncoming(TeamsPacket.class);
            var teamA = a.process().teamManager().createTeam("same");
            first.setTeam(teamA);
            aTeams.assertCount(2);
            bTeams.assertEmpty();
            var teamB = b.process().teamManager().createTeam("same");
            second.setTeam(teamB);
            assertEquals(List.of(first), List.copyOf(teamA.getPlayers()));
            assertEquals(List.of(second), List.copyOf(teamB.getPlayers()));
            assertThrows(IllegalArgumentException.class, () -> first.setTeam(teamB));
            assertSame(teamA, first.getTeam());
            assertThrows(IllegalArgumentException.class, () -> b.process().teamManager().deleteTeam(teamA));

            var bar = BossBar.bossBar(Component.text("shared"), 0.5f, BossBar.Color.BLUE, BossBar.Overlay.PROGRESS);
            PacketGroupingAudience.of(List.of(first, second)).showBossBar(bar);
            assertEquals(List.of(first), List.copyOf(a.process().bossBarManager().getBossBarViewers(bar)));
            assertEquals(List.of(second), List.copyOf(b.process().bossBarManager().getBossBarViewers(bar)));
            assertThrows(IllegalArgumentException.class, () -> a.process().bossBarManager().addBossBar(second, bar));
            a.process().audiences().registry().register(Key.key("test:custom"), first);
            assertTrue(b.process().audiences().registry().isEmpty());
            var clicksA = new AtomicInteger();
            var clicksB = new AtomicInteger();
            var clickA = a.process().clickCallbackManager().createClickEvent(_ -> clicksA.incrementAndGet(), ClickCallback.Options.builder().uses(2).build());
            var clickB = b.process().clickCallbackManager().createClickEvent(_ -> clicksB.incrementAndGet(), ClickCallback.Options.builder().uses(2).build());
            var payloadA = (ClickEvent.Payload.Custom) clickA.payload();
            var packetA = new ClientCustomClickActionPacket(payloadA.key(), ((BinaryTagHolderImpl) payloadA.nbt()).nbt());
            var payloadB = (ClickEvent.Payload.Custom) clickB.payload();
            var packetB = new ClientCustomClickActionPacket(payloadB.key(), ((BinaryTagHolderImpl) payloadB.nbt()).nbt());
            assertThrows(IllegalArgumentException.class, () -> a.process().clickCallbackManager().consumeCustomClick(second, packetA));
            b.process().clickCallbackManager().consumeCustomClick(second, packetA);
            assertEquals(0, clicksA.get());
            a.process().clickCallbackManager().consumeCustomClick(first, packetA);
            assertEquals(1, clicksA.get());
            a.process().close();
            assertNull(AdvancementTab.getTabs(first));
            assertEquals(Set.of(tabB), AdvancementTab.getTabs(second));
            assertTrue(tabB.isViewer(second));
            a.process().clickCallbackManager().consumeCustomClick(first, packetA);
            assertEquals(1, clicksA.get());
            b.process().clickCallbackManager().consumeCustomClick(second, packetB);
            assertEquals(1, clicksB.get());
            assertTrue(a.process().audiences().registry().isEmpty());
            assertTrue(a.process().bossBarManager().getBossBarViewers(bar).isEmpty());
            var closedPackets = ac.trackIncoming(BossBarPacket.class);
            var livePackets = bc.trackIncoming(BossBarPacket.class);
            bar.name(Component.text("still live"));
            closedPackets.assertEmpty();
            livePackets.assertSingle(packet -> assertTrue(packet.action() instanceof BossBarPacket.UpdateTitleAction));
            var liveMessages = bc.trackIncoming(ActionBarPacket.class);
            var command = new Command("after-close");
            command.setDefaultExecutor((_, context) -> context.process().audiences().players().sendActionBar(Component.text("still live")));
            b.process().commandManager().register(command);
            b.process().commandManager().executeServerCommand("after-close");
            liveMessages.assertSingle();
        }
    }

    @Test
    void inventoryEventsEnchantmentsAndDamageUseOwnedRegistries() {
        try (var pair = new ServerProcessPair(); var a = Env.create(pair.first()); var b = Env.create(pair.second())) {
            var ac = a.createConnection();
            var bc = b.createConnection();
            var first = ac.connect(a.createEmptyInstance(), Pos.ZERO);
            var second = bc.connect(b.createEmptyInstance(), Pos.ZERO);
            var firstChanges = new AtomicInteger();
            var secondChanges = new AtomicInteger();
            a.process().eventHandler().addListener(InventoryItemChangeEvent.class, _ -> firstChanges.incrementAndGet());
            b.process().eventHandler().addListener(InventoryItemChangeEvent.class, _ -> secondChanges.incrementAndGet());
            var inventory = new Inventory(b.process(), InventoryType.HOPPER, "second");
            var localChanges = new AtomicInteger();
            inventory.eventNode().addListener(InventoryItemChangeEvent.class, _ -> localChanges.incrementAndGet());
            second.openInventory(inventory);
            inventory.setItemStack(0, ItemStack.of(Material.DIAMOND));
            assertEquals(0, firstChanges.get());
            assertEquals(1, secondChanges.get());
            assertEquals(1, localChanges.get());
            assertThrows(IllegalArgumentException.class, () -> first.openInventory(inventory));
            assertThrows(IllegalArgumentException.class, () -> inventory.leftClick(first, 0));
            assertEquals(ItemStack.of(Material.DIAMOND), inventory.getItemStack(0));
            assertThrows(IllegalArgumentException.class, () -> a.process().eventHandler().call(new InventoryItemChangeEvent(inventory, 0, ItemStack.AIR, ItemStack.AIR)));

            var enchantment = a.process().registries().enchantment().get(Enchantment.SHARPNESS);
            var firstKey = a.process().registries().enchantment().register("test:shared", enchantment);
            b.process().registries().enchantment().register("test:padding", enchantment);
            var secondKey = b.process().registries().enchantment().register("test:shared", enchantment);
            assertNotEquals(a.process().registries().enchantment().getId(firstKey), b.process().registries().enchantment().getId(secondKey));
            var table = new EnchantmentTableInventory(b.process(), "second");
            second.openInventory(table);
            var properties = bc.trackIncoming(WindowPropertyPacket.class);
            table.setEnchantmentShown(EnchantmentTableInventory.EnchantmentSlot.TOP, secondKey);
            properties.assertSingle(packet -> assertEquals(b.process().registries().enchantment().getId(secondKey), packet.value()));
            assertEquals(secondKey, table.getEnchantmentShown(EnchantmentTableInventory.EnchantmentSlot.TOP));

            var firstType = a.process().registries().damageType().register("test:shared", DamageType.create("first", "never", 0, null, null));
            b.process().registries().damageType().register("test:padding", DamageType.create("padding", "never", 0, null, null));
            var secondType = b.process().registries().damageType().register("test:shared", DamageType.create("second", "never", 0, null, null));
            var damage = new Damage(b.process(), secondType, null, null, null, 1);
            assertNotEquals(a.process().registries().damageType().getId(firstType), damage.getTypeId());
            assertEquals(Component.translatable("death.attack.second"), damage.buildDeathScreenText(second));
            var damagePackets = bc.trackIncoming(DamageEventPacket.class);
            assertTrue(second.damage(damage));
            damagePackets.assertSingle(packet -> assertEquals(secondType, packet.damageType()));
            assertThrows(IllegalArgumentException.class, () -> first.damage(damage));
        }
    }
}

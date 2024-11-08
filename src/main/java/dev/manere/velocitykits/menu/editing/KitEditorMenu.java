package dev.manere.velocitykits.menu.editing;

import dev.manere.utils.item.ItemBuilder;
import dev.manere.utils.menu.Button;
import dev.manere.utils.menu.normal.Menu;
import dev.manere.utils.registration.Registrar;
import dev.manere.utils.scheduler.Schedulers;
import dev.manere.utils.text.color.TextStyle;
import dev.manere.velocitykits.storage.kit.Kit;
import org.bukkit.Material;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.HandlerList;
import org.bukkit.event.Listener;
import org.bukkit.event.inventory.InventoryCloseEvent;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.ItemStack;

import java.util.*;

public class KitEditorMenu implements dev.manere.velocitykits.menu.Menu<Menu>, Listener {
    private final dev.manere.utils.menu.normal.Menu menuBuilder;
    private final int kitNumber;
    public KitEditorMenu(Player player, int kitNumber) {
        this.menuBuilder = dev.manere.utils.menu.normal.Menu.menu(TextStyle.color("Kit Editor"), 54);
        this.kitNumber = kitNumber;

        Registrar.events(this);
        init(player);
    }

    @Override
    public void init(Player player) {
        Schedulers.sync().execute(task -> Kit.contentsAsync(player, kitNumber, kitContents -> {
            for (int slot = 0; slot < 41; slot++) {
                ItemStack item = kitContents.getOrDefault(slot, new ItemStack(Material.AIR));
                builder().getInventory().setItem(slot, item);
            }
        }), 1);

        List<Integer> border = new ArrayList<>(List.of(
                46, 47, 48, 49, 50, 51, 52, 53
        ));

        border.forEach(slot -> {
            if (this.menuBuilder.button(slot) == null) {
                button(slot, Button.button()
                        .item(ItemBuilder.item(Material.BLACK_STAINED_GLASS_PANE)
                                .name(TextStyle.color(" ")))
                        .onClick(event -> event.setCancelled(true)));
            }
        });

        List<Integer> armorAndHand = new ArrayList<>(List.of(
                41, 42, 43, 44
        ));

        armorAndHand.forEach(slot -> {
            if (this.menuBuilder.button(slot) == null) {
                button(slot, Button.button()
                        .item(ItemBuilder.item(Material.BLACK_STAINED_GLASS_PANE)
                                .name(TextStyle.color(" <#579af7>← ARMOR + OFFHAND")))
                        .onClick(event -> event.setCancelled(true)));
            }
        });

        button(45, Button.button()
                .item(ItemBuilder.item(Material.OAK_DOOR)
                        .name(TextStyle.color("<red>Exit")))
                .onClick(event -> {
                    event.setCancelled(true);

                    player.performCommand("k");
                }));

        button(53, Button.button()
                .item(ItemBuilder.item(Material.BARRIER)
                        .name(TextStyle.color("<#579af7>Close Menu")))
                .onClick(event -> {
                    event.setCancelled(true);

                    player.closeInventory();
                }));

        button(48, Button.button()
                .item(ItemBuilder.item(Material.RED_DYE)
                        .name(TextStyle.color("<#579af7>Clear/Reset Kit")))
                .onClick(event -> {
                    event.setCancelled(true);

                    List<Integer> possibilities = new ArrayList<>(List.of(
                            0,  1,  2,  3,  4,  5,  6,  7,  8,
                            9,  10, 11, 12, 13, 14, 15, 16, 17,
                            18, 19, 20, 21, 22, 23, 24, 25, 26,
                            27, 28, 29, 30, 31, 32, 33, 34, 35,
                            36, 37, 38, 39, 40
                    ));

                    possibilities.forEach(slot -> event.getInventory().setItem(slot, new ItemStack(Material.AIR)));

                    player.sendActionBar(TextStyle.color("<#00ff00>You have cleared/reset your kit."));
                }));

        button(50, Button.button()
                .item(ItemBuilder.item(Material.CHEST)
                        .name(TextStyle.color("<#579af7>Import Inventory to Kit")))
                .onClick(event -> {
                    event.setCancelled(true);

                    List<Integer> possibilities = new ArrayList<>(List.of(
                            0,  1,  2,  3,  4,  5,  6,  7,  8,
                            9,  10, 11, 12, 13, 14, 15, 16, 17,
                            18, 19, 20, 21, 22, 23, 24, 25, 26,
                            27, 28, 29, 30, 31, 32, 33, 34, 35,
                            36, 37, 38, 39, 40
                    ));

                    possibilities.forEach(slot -> {
                        ItemStack stack = player.getInventory().getItem(slot);

                        if (stack != null) {
                            event.getInventory().setItem(slot, stack);
                        }
                    });

                    player.sendActionBar(TextStyle.color("<#00ff00>Imported your inventory."));
                }));
    }

    @Override
    public dev.manere.utils.menu.normal.Menu builder() {
        return this.menuBuilder;
    }

    @Override
    @EventHandler
    public void onClose(InventoryCloseEvent event) {
        if (!event.getInventory().equals(builder().getInventory())) return;

        Inventory eventInventory = event.getInventory();
        Player player = (Player) event.getPlayer();

        player.sendActionBar(TextStyle.color("<#00ff00>Kit saved successfully!"));

        Map<Integer, ItemStack> contents = new HashMap<>();
        for (int i = 0; i < 41; i++) {
            ItemStack stack = eventInventory.getItem(i);

            contents.put(i, Objects.requireNonNullElseGet(stack, () -> new ItemStack(Material.AIR)));
        }

        Kit.saveAsync(player, kitNumber, contents);

        Schedulers.async().execute(task -> HandlerList.unregisterAll(this), 10);
    }
}

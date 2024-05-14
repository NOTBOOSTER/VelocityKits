package dev.manere.velocitykits.storage.kit;

import dev.manere.utils.library.Utils;
import dev.manere.utils.scheduler.Schedulers;
import dev.manere.utils.serializers.Serializers;
import dev.manere.utils.sql.connection.SQLConnector;
import dev.manere.utils.sql.enums.PrimaryColumn;
import dev.manere.utils.text.color.TextStyle;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.entity.Player;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.ItemStack;
import org.bukkit.plugin.Plugin;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class Kit {
    private static Connection connection;
    private static File yamlFile;
    private static FileConfiguration yamlConfig;

    public static void initialize(Plugin plugin) {
        String storageType = plugin.getConfig().getString("storage.type", "yaml");
        if ("mysql".equals(storageType)) {
            setupSQL(plugin);
        } else {
            setupYAML(plugin);
        }
    }

    private static void setupSQL(Plugin plugin) {
        connection = SQLConnector.of()
                .authentication()
                .host(plugin.getConfig().getString("sql.host"))
                .port(plugin.getConfig().getInt("sql.port"))
                .username(plugin.getConfig().getString("sql.username"))
                .password(plugin.getConfig().getString("sql.password"))
                .database(plugin.getConfig().getString("sql.database"))
                .build()
                .connect();

        try {
            String table = SQLTableBuilder.of()
                    .name("velocity_kits")
                    .column("player_uuid", "VARCHAR(36) NOT NULL", PrimaryColumn.TRUE)
                    .column("kit_number", "INT NOT NULL", PrimaryColumn.TRUE)
                    .column("contents", "TEXT(65535) NOT NULL", PrimaryColumn.FALSE)
                    .build();

            connection.prepareStatement(table).executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void setupYAML(Plugin plugin) {
        yamlFile = new File(plugin.getDataFolder(), "kits.yml");
        if (!yamlFile.exists()) {
            try {
                yamlFile.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException("Could not create kits YAML file.", e);
            }
        }
        yamlConfig = YamlConfiguration.loadConfiguration(yamlFile);
    }

    public static void delete(String playerUUID, int kitNumber) {
        String storageType = Utils.plugin().getConfig().getString("storage.type", "yaml");
        if ("mysql".equals(storageType)) {
            deleteSQL(playerUUID, kitNumber);
        } else {
            deleteYAML(playerUUID, kitNumber);
        }
    }

    private static void deleteSQL(String playerUUID, int kitNumber) {
        try (PreparedStatement stmt = connection.prepareStatement("DELETE FROM velocity_kits WHERE player_uuid = ? AND kit_number = ?")) {
            stmt.setString(1, playerUUID);
            stmt.setInt(2, kitNumber);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to delete kit from SQL database.", e);
        }
    }

    private static void deleteYAML(String playerUUID, int kitNumber) {
        String path = playerUUID + "." + kitNumber;
        yamlConfig.set(path, null);
        try {
            yamlConfig.save(yamlFile);
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete kit from YAML file.", e);
        }
    }

    public static void load(Player player, int kitNumber) {
        contentsAsync(player, kitNumber, contents -> {
            Inventory inventory = player.getInventory();
            inventory.clear();

            if (contents.isEmpty()) {
                player.sendActionBar(TextStyle.color("<#ff0000>That kit is empty!"));
                return;
            }

            player.sendActionBar(TextStyle.color("<#00ff00>Kit " + kitNumber + " has been loaded."));
            for (Map.Entry<Integer, ItemStack> entry : contents.entrySet()) {
                inventory.setItem(entry.getKey(), entry.getValue());
            }
        });
    }

    public static void saveAsync(Player player, int kitNumber, Map<Integer, ItemStack> contents) {
        String playerUUID = player.getUniqueId().toString();
        Schedulers.async().execute(() -> save(playerUUID, kitNumber, contents));
    }

    public static void save(String playerUUID, int kitNumber, Map<Integer, ItemStack> contents) {
        String storageType = Utils.plugin().getConfig().getString("storage.type", "yaml");
        if ("mysql".equals(storageType)) {
            saveSQL(playerUUID, kitNumber, contents);
        } else {
            saveYAML(playerUUID, kitNumber, contents);
        }
    }

    private static void saveSQL(String playerUUID, int kitNumber, Map<Integer, ItemStack> contents) {
        String data = Serializers.base64().serializeItemStacks(contents);
        try (PreparedStatement stmt = connection.prepareStatement(
                "REPLACE INTO velocity_kits (player_uuid, kit_number, contents) VALUES (?, ?, ?)")) {
            stmt.setString(1, playerUUID);
            stmt.setInt(2, kitNumber);
            stmt.setString(3, data);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to save kit to SQL database.", e);
        }
    }

    private static void saveYAML(String playerUUID, int kitNumber, Map<Integer, ItemStack> contents) {
        String path = playerUUID + "." + kitNumber;
        yamlConfig.set(path, contents);
        try {
            yamlConfig.save(yamlFile);
        } catch (IOException e) {
            throw new RuntimeException("Failed to save kit to YAML file.", e);
        }
    }

    private static void contentsAsync(Player player, int kitNumber, Consumer<Map<Integer, ItemStack>> callback) {
        Schedulers.async().execute(() -> {
            Map<Integer, ItemStack> kitContents = contents(player.getUniqueId().toString(), kitNumber);
            Schedulers.sync().execute(() -> callback.accept(kitContents));
        });
    }

    public static void contentsAsync(String playerUUID, int kitNumber, Consumer<Map<Integer, ItemStack>> callback) {
        Schedulers.async().execute(() -> {
            Map<Integer, ItemStack> kitContents = contents(playerUUID, kitNumber);
            Schedulers.sync().execute(() -> callback.accept(kitContents));
        });
    }

    private static Map<Integer, ItemStack> contents(String playerUUID, int kitNumber) {
        ResultSet rs = null;
        try {
            try (PreparedStatement stmt = connection.prepareStatement("SELECT contents FROM velocity_kits WHERE player_uuid = ? AND kit_number = ?")) {
                stmt.setString(1, playerUUID);
                stmt.setInt(2, kitNumber);

                rs = stmt.executeQuery();

                if (rs.next()) {
                    String data = rs.getString("contents");
                    return Serializers.base64().deserializeItemStackMap(data);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to retrieve kit contents from database.", e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return new HashMap<>();
    }

    public static void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error closing SQL connection.", e);
        }
    }
}

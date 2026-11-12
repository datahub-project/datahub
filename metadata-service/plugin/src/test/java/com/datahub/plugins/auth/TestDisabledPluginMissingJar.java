package com.datahub.plugins.auth;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.datahub.plugins.common.PluginConfig;
import com.datahub.plugins.common.PluginType;
import com.datahub.plugins.configuration.Config;
import com.datahub.plugins.configuration.ConfigProvider;
import com.datahub.plugins.factory.PluginConfigFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * A plugin entry with {@code enabled: false} must not require its jar: GMS loads auth plugin
 * configs at startup, and a disabled plugin whose jar is not mounted should be skipped rather than
 * fail the whole load. An enabled plugin without its jar must still fail.
 */
public class TestDisabledPluginMissingJar {

  private final List<Path> tempDirs = new ArrayList<>();

  @AfterMethod
  public void deleteTempDirs() throws IOException {
    for (Path dir : tempDirs) {
      try (Stream<Path> paths = Files.walk(dir)) {
        for (Path p : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
          Files.delete(p);
        }
      }
    }
    tempDirs.clear();
  }

  private Path tempDir(String prefix) throws IOException {
    Path dir = Files.createTempDirectory(prefix);
    tempDirs.add(dir);
    return dir;
  }

  @DataProvider(name = "pluginTypes")
  public Object[][] pluginTypes() {
    return new Object[][] {
      {PluginType.AUTHENTICATOR, "authenticator"}, {PluginType.AUTHORIZER, "authorizer"}
    };
  }

  @Test(dataProvider = "pluginTypes")
  public void disabledPluginWithMissingJarIsSkipped(PluginType type, String yamlType)
      throws IOException {
    Path baseDir = tempDir("disabled-plugin-missing-jar");
    writeConfig(baseDir, pluginEntry("disabled-plugin", yamlType, false));

    List<PluginConfig> configs = load(baseDir, type);

    assertTrue(configs.isEmpty(), "disabled plugin should be dropped, got " + configs);
  }

  @Test(dataProvider = "pluginTypes")
  public void enabledPluginWithMissingJarStillFails(PluginType type, String yamlType)
      throws IOException {
    Path baseDir = tempDir("enabled-plugin-missing-jar");
    writeConfig(baseDir, pluginEntry("enabled-plugin", yamlType, true));
    Path expectedJar =
        baseDir.toAbsolutePath().resolve("enabled-plugin").resolve("enabled-plugin.jar");

    IllegalArgumentException e =
        expectThrows(IllegalArgumentException.class, () -> load(baseDir, type));
    assertTrue(
        e.getMessage().contains(expectedJar.toString()),
        "error should name the missing jar: " + e.getMessage());
  }

  @Test(dataProvider = "pluginTypes")
  public void disabledPluginWithMissingJarDoesNotHideEnabledPlugin(PluginType type, String yamlType)
      throws IOException {
    Path baseDir = tempDir("mixed-plugins");
    Path enabledHome = Files.createDirectories(baseDir.resolve("enabled-plugin"));
    Files.createFile(enabledHome.resolve("enabled-plugin.jar"));
    writeConfig(
        baseDir,
        pluginEntry("disabled-plugin", yamlType, false)
            + pluginEntry("enabled-plugin", yamlType, true));

    List<PluginConfig> configs = load(baseDir, type);

    assertEquals(
        configs.stream().map(PluginConfig::getName).collect(Collectors.toList()),
        List.of("enabled-plugin"));
    assertEquals(
        configs.get(0).getPluginJarPath(),
        baseDir.toAbsolutePath().resolve("enabled-plugin").resolve("enabled-plugin.jar"));
  }

  private static List<PluginConfig> load(Path baseDir, PluginType type) {
    Config config = new ConfigProvider(baseDir).load().orElseThrow();
    return new PluginConfigFactory(config).loadPluginConfigs(type);
  }

  private static String pluginEntry(String name, String yamlType, boolean enabled) {
    return String.format(
        "  - name: \"%s\"%n"
            + "    type: \"%s\"%n"
            + "    enabled: \"%s\"%n"
            + "    params:%n"
            + "      className: \"com.example.Plugin\"%n",
        name, yamlType, enabled);
  }

  private static void writeConfig(Path baseDir, String entries) throws IOException {
    Files.writeString(baseDir.resolve(ConfigProvider.CONFIG_FILE_NAME), "plugins:\n" + entries);
  }
}

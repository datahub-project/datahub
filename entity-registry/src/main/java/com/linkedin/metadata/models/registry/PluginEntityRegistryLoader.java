package com.linkedin.metadata.models.registry;

import com.linkedin.metadata.aspect.plugins.PluginFactory;
import com.linkedin.metadata.aspect.plugins.config.PluginConfiguration;
import com.linkedin.metadata.models.registry.config.EntityRegistryLoadResult;
import com.linkedin.metadata.models.registry.config.LoadStatus;
import com.linkedin.util.Pair;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.maven.artifact.versioning.ComparableVersion;

@Slf4j
public class PluginEntityRegistryLoader {
  private static int _MAXLOADFAILURES = 5;
  private final Boolean scanningEnabled;
  private final String pluginDirectory;
  private final int loadDelaySeconds;
  // Registry Name -> Registry Version -> (Registry, LoadResult)
  private final Map<String, Map<ComparableVersion, Pair<EntityRegistry, EntityRegistryLoadResult>>>
      patchRegistries;
  private MergedEntityRegistry mergedEntityRegistry;

  @Nullable
  private final BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory>
      pluginFactoryProvider;

  private boolean started = false;
  private final Lock lock = new ReentrantLock();
  private final Condition initialized = lock.newCondition();
  private boolean booted = false;
  private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

  public PluginEntityRegistryLoader(
      String pluginDirectory,
      int loadDelaySeconds,
      @Nullable
          BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactoryProvider) {
    File directory = new File(pluginDirectory);
    if (!directory.exists() || !directory.isDirectory()) {
      log.warn(
          "{} directory does not exist or is not a directory. Plugin scanning will be disabled.",
          directory);
      scanningEnabled = false;
    } else {
      scanningEnabled = true;
    }
    this.pluginDirectory = pluginDirectory;
    this.patchRegistries = new HashMap<>();
    this.loadDelaySeconds = loadDelaySeconds;
    this.pluginFactoryProvider = pluginFactoryProvider;
  }

  public Map<String, Map<ComparableVersion, Pair<EntityRegistry, EntityRegistryLoadResult>>>
      getPatchRegistries() {
    return patchRegistries;
  }

  public PluginEntityRegistryLoader withBaseRegistry(MergedEntityRegistry baseEntityRegistry) {
    this.mergedEntityRegistry = baseEntityRegistry;
    return this;
  }

  public PluginEntityRegistryLoader start(boolean waitForInitialization)
      throws InterruptedException {
    if (started) {
      log.warn("Already started!. Skipping");
      return this;
    }
    if (!scanningEnabled) {
      return this;
    }

    executorService.scheduleAtFixedRate(
        () -> {
          lock.lock();
          try {
            Path rootPath = Paths.get(this.pluginDirectory);
            int rootDepth = rootPath.getNameCount();
            List<Path> paths =
                Files.walk(rootPath, 2)
                    .filter(x -> x.getNameCount() - rootDepth == 2)
                    .collect(Collectors.toList());
            log.debug("Size of list {}", paths.size());
            log.debug(
                "Paths : {}",
                paths.stream().map(x -> x.toString() + ";").collect(Collectors.joining()));
            List<Path> versionedPaths =
                paths.stream()
                    .filter(
                        path -> {
                          try {
                            ComparableVersion comparableVersion =
                                new ComparableVersion(path.getName(rootDepth + 1).toString());
                            return true;
                          } catch (Exception e) {
                            log.warn(
                                String.format(
                                    "Will skip %s since we weren't able to parse a legal version from it",
                                    path.toString()));
                            return false;
                          }
                        })
                    .sorted(
                        (path1, path2) -> {
                          if (path1.getName(rootDepth).equals(path2.getName(rootDepth))) {
                            return new ComparableVersion(path1.getName(rootDepth + 1).toString())
                                .compareTo(
                                    new ComparableVersion(path2.getName(rootDepth + 1).toString()));
                          } else {
                            return path1.getName(rootDepth).compareTo(path2.getName(rootDepth));
                          }
                        })
                    .collect(Collectors.toList());
            log.debug(
                "Will be loading paths in this order {}",
                versionedPaths.stream().map(p -> p.toString()).collect(Collectors.joining(";")));

            versionedPaths.forEach(
                x ->
                    loadOneRegistry(
                        this.mergedEntityRegistry,
                        x.getName(rootDepth).toString(),
                        x.getName(rootDepth + 1).toString(),
                        x.toString()));
          } catch (Exception e) {
            log.warn("Failed to walk directory with exception", e);
          } finally {
            booted = true;
            initialized.signal();
            lock.unlock();
          }
        },
        0,
        loadDelaySeconds,
        TimeUnit.SECONDS);
    started = true;
    if (waitForInitialization) {
      lock.lock();
      try {
        while (!booted) {
          initialized.await(100, TimeUnit.SECONDS);
        }
      } finally {
        lock.unlock();
      }
    }
    return this;
  }

  private void loadOneRegistry(
      MergedEntityRegistry parentRegistry,
      String registryName,
      String registryVersionStr,
      String patchDirectory) {
    ComparableVersion registryVersion = new ComparableVersion("0.0.0-dev");
    try {
      ComparableVersion maybeVersion = new ComparableVersion(registryVersionStr);
      log.debug("{}: Found registry version {}", this, maybeVersion);
      registryVersion = maybeVersion;
    } catch (IllegalArgumentException ie) {
      log.warn(
          "Found un-parseable registry version {}, will default to {}",
          registryVersionStr,
          registryVersion);
    }

    if (registryExists(registryName, registryVersion)) {
      log.debug(
          "Registry {}:{} already exists. Skipping loading...", registryName, registryVersion);
      return;
    } else {
      log.info("{}: Registry {}:{} discovered. Loading...", this, registryName, registryVersion);
    }

    EntityRegistryLoadResult.EntityRegistryLoadResultBuilder loadResultBuilder =
        EntityRegistryLoadResult.builder().registryLocation(patchDirectory);
    EntityRegistry entityRegistry = null;
    try {
      entityRegistry =
          new PatchEntityRegistry(
              patchDirectory, registryName, registryVersion, pluginFactoryProvider);
      parentRegistry.apply(entityRegistry);
      loadResultBuilder.loadResult(LoadStatus.SUCCESS);

      // Load plugin information
      loadResultBuilder.plugins(entityRegistry.getPluginFactory().getPluginLoadResult());

      log.info("Loaded registry {} successfully", entityRegistry);
    } catch (Exception | EntityRegistryException e) {
      log.error("{}: Failed to load registry {} with {}", this, registryName, e.getMessage(), e);
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      loadResultBuilder.loadResult(LoadStatus.FAILURE).failureReason(sw.toString()).failureCount(1);
    }

    addLoadResult(registryName, registryVersion, loadResultBuilder.build(), entityRegistry);
  }

  private boolean registryExists(String registryName, ComparableVersion registryVersion) {
    Map<ComparableVersion, Pair<EntityRegistry, EntityRegistryLoadResult>> nameTree =
        patchRegistries.getOrDefault(registryName, new HashMap<>());
    if (nameTree.containsKey(registryVersion)
        && ((nameTree.get(registryVersion).getSecond().getLoadResult() == LoadStatus.SUCCESS)
            || (nameTree.get(registryVersion).getSecond().getFailureCount() == _MAXLOADFAILURES))) {
      return true;
    }
    return false;
  }

  private void addLoadResult(
      String registryName,
      ComparableVersion semanticVersion,
      EntityRegistryLoadResult loadResult,
      EntityRegistry e) {
    Map<ComparableVersion, Pair<EntityRegistry, EntityRegistryLoadResult>> nameTree =
        patchRegistries.getOrDefault(registryName, new HashMap<>());
    if (nameTree.containsKey(semanticVersion)) {
      if ((loadResult.getLoadResult() == LoadStatus.FAILURE)
          && (nameTree.get(semanticVersion).getSecond().getLoadResult() == LoadStatus.FAILURE)) {
        // previous load and current loads are both failures
        loadResult.setFailureCount(nameTree.get(semanticVersion).getSecond().getFailureCount() + 1);
        if (loadResult.getFailureCount() == _MAXLOADFAILURES) {
          // Abandoning this registry version forever
          log.error(
              "Tried {} times. Failed to load registry {} with {}",
              loadResult.getFailureCount(),
              registryName,
              loadResult.getFailureReason());
        }
      }
      log.warn(
          String.format(
              "Attempt %d to re-load registry %s: %s",
              loadResult.getFailureCount(), registryName, semanticVersion));
    }
    nameTree.put(semanticVersion, new Pair<>(e, loadResult));
    patchRegistries.put(registryName, nameTree);
  }
}

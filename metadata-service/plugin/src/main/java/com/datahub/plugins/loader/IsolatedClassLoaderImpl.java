package com.datahub.plugins.loader;

import com.datahub.plugins.Plugin;
import com.datahub.plugins.common.IsolatedClassLoader;
import com.datahub.plugins.common.PluginConfigWithJar;
import com.datahub.plugins.common.PluginPermissionManager;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;


/**
 * IsolatedClassLoader to load custom implementation of DataHub Plugins.
 */
@Slf4j
public class IsolatedClassLoaderImpl extends ClassLoader implements IsolatedClassLoader {
  public static final String EXECUTION_DIR = "__run__";
  private final PluginPermissionManager _pluginPermissionManager;

  private final PluginConfigWithJar _pluginConfig;

  private final List<ClassLoader> _classLoaders = new ArrayList<>(2);

  private Map<String, ZipEntry> _classPathVsZipEntry;

  private JarFile _pluginJarRef;

  private final Path _executionDirectory;

  public IsolatedClassLoaderImpl(PluginPermissionManager pluginPermissionManager, PluginConfigWithJar pluginToLoad,
      ClassLoader... applicationClassLoaders) {
    this._pluginPermissionManager = pluginPermissionManager;
    this._pluginConfig = pluginToLoad;
    this._classLoaders.add(this.getClass().getClassLoader()); // then application class-loader
    this._classLoaders.addAll(Arrays.asList(applicationClassLoaders)); // if any extra class loaders
    this._executionDirectory =
        Paths.get(pluginToLoad.getPluginHomeDirectory().toString(), EXECUTION_DIR); // to store .so files i.e. libraries
    try {
      this.createJarEntryMap();
    } catch (IOException e) {
      // This would occur if we don't have permission on directory and chances of this is close to zero, hence catching
      // this checked exception and throwing runtime exception
      // to make caller code more readable
      log.warn(String.format("Unable to load jar file %s for plugin %s", pluginToLoad.getPluginJarPath(),
          pluginToLoad.getName()));
      throw new RuntimeException(e);
    }
  }

  private void createJarEntryMap() throws IOException {
    this._pluginJarRef = new JarFile(this._pluginConfig.getPluginJarPath().toFile());
    this._classPathVsZipEntry = new HashMap<>();
    for (Enumeration<JarEntry> enums = this._pluginJarRef.entries(); enums.hasMoreElements(); ) {
      JarEntry entry = enums.nextElement();
      if (entry.getName().endsWith("/")) {
        // we don't want to keep directories in map
        continue;
      }
      this._classPathVsZipEntry.put(entry.getName(), entry);
    }
  }

  /**
   * Load plugin class from jar given in pluginToLoad parameter and return instance of class which implements Plugin interface.
   * This method verifies whether loaded plugin is assignable to expectedInstanceOf class
   * @param expectedInstanceOf class instance of interface caller is expecting
   * @return Instance of Plugin
   * @throws ClassNotFoundException className parameter available in Plugin configuration is not found
   */
  @Nonnull
  public Plugin instantiatePlugin(Class<? extends Plugin> expectedInstanceOf) throws ClassNotFoundException {
    Class<?> clazz = this.loadClass(this._pluginConfig.getClassName(), true);

    try {
      log.debug("Creating instance of plugin {}", this._pluginConfig.getClassName());
      Plugin plugin = (Plugin) clazz.newInstance();
      // Check loaded plugin has implemented the proper implementation of child interface
      if (!expectedInstanceOf.isAssignableFrom(clazz)) {
        throw new InstantiationException(
            String.format("In plugin %s, the class %s has not implemented the interface %s",
                this._pluginConfig.getName(), plugin.getClass().getCanonicalName(),
                expectedInstanceOf.getCanonicalName()));
      }
      log.debug("Successfully created instance of plugin {}", this._pluginConfig.getClassName());
      return plugin;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private String classNameToPath(String resourceName) {
    // in-case of java class , we need to append the .class to last element
    return resourceName.replaceAll("\\.", "/") + ".class";
  }

  private byte[] getClassData(ZipEntry zipEntry) {
    try (InputStream ins = this._pluginJarRef.getInputStream(zipEntry);
        ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[4096];
      int bytesNumRead;
      while ((bytesNumRead = ins.read(buffer)) != -1) {
        baos.write(buffer, 0, bytesNumRead);
      }
      return baos.toByteArray();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return new byte[]{};
  }

  @Override
  protected Class<?> loadClass(String s, boolean b) throws ClassNotFoundException {
    log.debug(String.format("Load class %s", s));
    String path = this.classNameToPath(s);
    log.debug(String.format("File path %s", path));
    // Check if requested class is available in plugin jar entries
    if (!this._classPathVsZipEntry.containsKey(path)) {
      // Try to load using Application class loader
      log.debug(String.format("Class %s not found in plugin jar, trying application class loader chain", s));
      for (ClassLoader classLoader : this._classLoaders) {
        try {
          log.debug("Looking in ClassLoader {}", classLoader.getClass().getName());
          return classLoader.loadClass(s);
        } catch (ClassNotFoundException classNotFoundException) {
          // Pass it and let search in next ClassLoader
        }
      }
      throw new ClassNotFoundException();
    }

    byte[] classBytes = getClassData(this._classPathVsZipEntry.get(path));
    if (classBytes.length == 0) {
      throw new ClassNotFoundException();
    }
    ProtectionDomain protectionDomain =
        this._pluginPermissionManager.createProtectionDomain(this._pluginConfig.getPluginHomeDirectory());
    return defineClass(s, classBytes, 0, classBytes.length, protectionDomain);
  }

  @Override
  public URL getResource(String s) {
    log.debug("Get resource {}", s);
    return this.findResource(s);
  }

  @Override
  public Enumeration<URL> getResources(String s) throws IOException {
    URL url = this.getResource(s);
    if (url == null) {
      log.debug("Returning empty enumeration");
      return Collections.emptyEnumeration();
    }
    List<URL> urls = new ArrayList<>(1);
    urls.add(url);
    return Collections.enumeration(urls);
  }

  @Override
  public InputStream getResourceAsStream(String s) {
    log.debug("Resource as stream = {}", s);
    try {
      URL url = this.findResource(s);
      if (url == null) {
        return null;
      }
      return url.openStream();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Look for resource in below order
   * - First search in plugin jar if not found
   * - then search in plugin directory if not found then return null
   * @param resource Resource to find
   * @return URL of the resource
   */
  @Override
  protected URL findResource(String resource) {
    URL url = null;
    String trimResource = StringUtils.strip(resource.trim(), "/");

    log.debug("Finding resource = {}", trimResource);
    // Look for resource in jar entries
    if (this._classPathVsZipEntry.containsKey(trimResource)) {
      StringBuilder builder = new StringBuilder();
      builder.append("jar:file:").append(this._pluginConfig.getPluginJarPath()).append("!/");
      builder.append(trimResource);
      try {
        log.debug("Resource {} is found in plugin jar at location {}", trimResource, builder);
        return new URL(builder.toString());
      } catch (MalformedURLException e) {
        throw new RuntimeException(e);
      }
    }
    // Check if resource is present in plugin directory
    try {
      try (Stream<Path> stream = Files.find(this._pluginConfig.getPluginHomeDirectory(), 1,
          ((path, basicFileAttributes) -> path.toFile().getName().equals(trimResource)))) {
        List<Path> resources = stream.collect(Collectors.toList());
        if (resources.size() > 0) {
          log.debug("Number of resources found {}", resources.size());
          log.debug("Resource {} is found in plugin directory", trimResource);
          url = resources.get(0).toUri().toURL();
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (url == null) {
      log.debug("Resource not found in plugin = {}", trimResource);
      log.debug("Trying application class loader chain");
      for (ClassLoader classLoader : this._classLoaders) {
        url = classLoader.getResource(trimResource);
        if (url != null) {
          log.debug("Resource found in ClassLoader = {}", classLoader.getClass().getName());
          break;
        }
      }
    }
    return url;
  }

  @Override
  protected String findLibrary(String s) {
    log.debug("Looking for library {}", s);
    Path destinationPath = Paths.get(this._executionDirectory.toString(), s);
    File file = destinationPath.toFile();

    // Check if already present
    if (file.exists()) {
      log.debug("Library found in execution directory");
      return destinationPath.toString();
    }

    if (!this._executionDirectory.toFile().exists()) {
      if (!this._executionDirectory.toFile().mkdirs()) {
        log.warn("Failed to create directory {}", this._executionDirectory);
        return null;
      }
    }
    // Look in plugin jar, plugin directory and chain of class loader
    URL url = this.findResource(s);
    if (url == null) {
      log.debug("Library not found");
      return null;
    }

    try {
      JarExtractor.write(url, destinationPath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    log.debug("Library found in ClassLoader");
    return destinationPath.toString();
  }

  @Override
  protected Enumeration<URL> findResources(String s) throws IOException {
    log.debug("Find resources = {}", s);
    URL url = this.findResource(s);
    if (url == null) {
      log.debug("Returning empty enumeration");
      return Collections.emptyEnumeration();
    }
    List<URL> urls = new ArrayList<>(1);
    urls.add(url);
    return Collections.enumeration(urls);
  }
}

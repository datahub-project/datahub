package com.linkedin.metadata.models;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.metadata.models.annotation.EventAnnotation;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

/**
 * Factory class to get a map of all entity schemas and aspect schemas under com.linkedin package
 * This lets us fetch the PDL data schema of an arbitrary entity or aspect based on their names
 */
@Slf4j
public class DataSchemaFactory {
  private final Map<String, DataSchema> entitySchemas;
  private final Map<String, DataSchema> aspectSchemas;
  private final Map<String, DataSchema> eventSchemas;

  private final Map<String, Class> aspectClasses;

  private static final String NAME_FIELD = "name";

  private static volatile DataSchemaFactory INSTANCE = null;
  private static final String[] DEFAULT_TOP_LEVEL_NAMESPACES =
      new String[] {"com", "org", "io", "datahub"};

  public DataSchemaFactory(boolean useOptimizedEntityLoading) {
    this(new String[] {"com.linkedin", "com.datahub"}, useOptimizedEntityLoading);
  }

  public DataSchemaFactory(String classPath) {
    this(new String[] {classPath}, false);
  }

  public DataSchemaFactory(String[] classPaths, boolean useOptimizedEntityLoading) {
    this(classPaths, null, useOptimizedEntityLoading);
  }

  /**
   * Construct a DataSchemaFactory with classes and schemas found under a specific folder. This will
   * only look for classes under the `com`, `org` or `datahub` top level namespaces.
   *
   * @param pluginLocation The location of the classes and schema files.
   */
  public static DataSchemaFactory withCustomClasspath(Path pluginLocation) throws IOException {
    return withCustomClasspath(pluginLocation, false);
  }

  public static DataSchemaFactory withCustomClasspath(
      Path pluginLocation, boolean useOptimizedLoading) throws IOException {
    if (pluginLocation == null) {
      // no custom classpath, just return the default factory
      return getInstance(useOptimizedLoading);
    }

    return new DataSchemaFactory(
        DEFAULT_TOP_LEVEL_NAMESPACES, getClassLoader(pluginLocation).get(), useOptimizedLoading);
  }

  public static Optional<ClassLoader> getClassLoader(@Nullable Path pluginLocation)
      throws IOException {
    if (pluginLocation == null) {
      return Optional.empty();
    } else {
      // first we load up classes from the classpath
      File pluginDir = pluginLocation.toFile();
      if (!pluginDir.exists()) {
        throw new RuntimeException(
            "Failed to find plugin directory "
                + pluginDir.getAbsolutePath()
                + ". Current directory is "
                + new File(".").getAbsolutePath());
      }
      List<URL> urls = new ArrayList<URL>();
      if (pluginDir.isDirectory()) {
        List<Path> jarFiles =
            Files.walk(pluginLocation)
                .filter(Files::isRegularFile)
                .filter(p -> p.toString().endsWith(".jar"))
                .collect(Collectors.toList());
        for (Path f : jarFiles) {
          URL url = f.toUri().toURL();
          if (url != null) {
            urls.add(url);
          }
        }
      } else {
        URL url = (pluginLocation.toUri().toURL());
        urls.add(url);
      }
      URL[] urlsArray = new URL[urls.size()];
      urls.toArray(urlsArray);
      URLClassLoader classLoader =
          new URLClassLoader(urlsArray, Thread.currentThread().getContextClassLoader());
      return Optional.of(classLoader);
    }
  }

  /**
   * Construct a DataSchemaFactory with a custom class loader and a list of class namespaces to look
   * for entities and aspects.
   */
  public DataSchemaFactory(
      String[] classNamespaces, ClassLoader customClassLoader, boolean useOptimizedEntityLoading) {
    entitySchemas = new ConcurrentHashMap<>();
    aspectSchemas = new ConcurrentHashMap<>();
    eventSchemas = new ConcurrentHashMap<>();
    aspectClasses = new ConcurrentHashMap<>();

    Set<Class<? extends RecordTemplate>> classes =
        loadClasses(classNamespaces, customClassLoader, useOptimizedEntityLoading);

    processSchemasInBatches(classes);
  }

  private Set<Class<? extends RecordTemplate>> loadClasses(
      String[] classNamespaces, ClassLoader customClassLoader, boolean useOptimisedLoading) {
    Set<Class<? extends RecordTemplate>> classes = new HashSet<>();
    if (useOptimisedLoading) {
      try {
        classes = loadPdlClassesOptimized();
      } catch (Exception e) {
        classes = loadPdlClasses(classNamespaces, customClassLoader);
      }
    }

    if (classes.isEmpty()) {
      log.error(
          "[DataSchemaFactory Error] zero classes returned from loadPdlClassesOptimized , falling back to normal loading");
      classes = loadPdlClasses(classNamespaces, customClassLoader);
    }
    return classes;
  }

  public static Set<Class<? extends RecordTemplate>> loadPdlClasses(
      String[] classNamespaces, ClassLoader customClassLoader) {
    Set<Class<? extends RecordTemplate>> classes = new HashSet<>();
    ClassLoader standardClassLoader = null;
    if (customClassLoader == null) {
      customClassLoader = Thread.currentThread().getContextClassLoader();
    } else {
      standardClassLoader = Thread.currentThread().getContextClassLoader();
    }
    // When using a custom classloader (especially URLClassLoader), we need to get URLs directly
    if (customClassLoader instanceof URLClassLoader) {
      URLClassLoader urlClassLoader = (URLClassLoader) customClassLoader;
      URL[] urls = urlClassLoader.getURLs();

      log.debug("Using URLClassLoader with {} URLs", urls.length);

      // Create a single Reflections instance with all URLs
      ConfigurationBuilder configBuilder =
          new ConfigurationBuilder()
              .setUrls(Arrays.asList(urls))
              .addClassLoader(urlClassLoader)
              .setScanners(new SubTypesScanner());

      // Add packages separately to avoid issues
      for (String pkg : classNamespaces) {
        configBuilder.forPackages(pkg);
      }

      Reflections reflections = new Reflections(configBuilder);
      classes.addAll(reflections.getSubTypesOf(RecordTemplate.class));

    } else {
      // Fallback to the original approach for non-URLClassLoader
      for (String namespace : classNamespaces) {
        log.debug("Reflections scanning {} namespace", namespace);

        // Use ClasspathHelper to get URLs for the package
        Collection<URL> packageUrls = ClasspathHelper.forPackage(namespace, customClassLoader);

        ConfigurationBuilder configBuilder =
            new ConfigurationBuilder()
                .setUrls(packageUrls)
                .addClassLoader(customClassLoader)
                .setScanners(new SubTypesScanner());

        Reflections reflections = new Reflections(configBuilder);
        classes.addAll(reflections.getSubTypesOf(RecordTemplate.class));
      }
    }

    log.debug("Found a total of {} RecordTemplate classes", classes.size());

    if (standardClassLoader != null) {
      Set<Class<? extends RecordTemplate>> stdClasses = new HashSet<>();
      try {
        for (String namespace : classNamespaces) {
          // Use ClasspathHelper to properly get URLs for standard classloader
          Collection<URL> packageUrls = ClasspathHelper.forPackage(namespace, standardClassLoader);

          if (!packageUrls.isEmpty()) {
            ConfigurationBuilder configBuilder =
                new ConfigurationBuilder()
                    .setUrls(packageUrls)
                    .addClassLoader(standardClassLoader)
                    .setScanners(new SubTypesScanner());

            Reflections reflections = new Reflections(configBuilder);
            stdClasses.addAll(reflections.getSubTypesOf(RecordTemplate.class));
          }
        }
        log.debug(
            "Standard ClassLoader found a total of {} RecordTemplate classes", stdClasses.size());
        classes.removeAll(stdClasses);
        log.debug("Finally found a total of {} RecordTemplate classes to inspect", classes.size());
      } catch (Exception e) {
        log.warn(
            "Failed to scan with standard classloader, continuing with custom classloader results only",
            e);
        // Continue without removing standard classes - not critical for functionality
      }
    }
    return classes;
  }

  public static Set<Class<? extends RecordTemplate>> loadPdlClassesOptimized() {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Set<Class<? extends RecordTemplate>> classes = new HashSet<>();

    try {
      Enumeration<URL> urls = cl.getResources("META-INF/pegasus-models.idx");

      while (urls.hasMoreElements()) {
        URL url = urls.nextElement();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
          reader
              .lines()
              .forEach(
                  name -> {
                    try {
                      Class<?> raw = Class.forName(name, false, cl);
                      if (!RecordTemplate.class.isAssignableFrom(raw)) {
                        return;
                      }
                      @SuppressWarnings("unchecked")
                      Class<? extends RecordTemplate> clazz = (Class<? extends RecordTemplate>) raw;

                      classes.add(clazz);
                    } catch (ClassNotFoundException e) {
                      log.error("Unable to load pegasus ids files ", e);
                      throw new RuntimeException("Failed loading " + name, e);
                    }
                  });
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed reading Pegasus model index", e);
    }

    return classes;
  }

  private Optional<String> getName(Map<String, Object> properties, String annotationName) {
    return Optional.ofNullable(properties.get(annotationName))
        .filter(obj -> Map.class.isAssignableFrom(obj.getClass()))
        .flatMap(obj -> Optional.ofNullable(((Map) obj).get(NAME_FIELD)).map(Object::toString));
  }

  /**
   * Process schemas in parallel batches to improve performance. Creates and submits batches
   * immediately for processing.
   */
  private void processSchemasInBatches(Set<Class<? extends RecordTemplate>> classes) {
    int batchSize = 200;
    int numThreads = Math.min(Runtime.getRuntime().availableProcessors(), 4);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    try {
      List<Future<?>> futures = new ArrayList<>();
      List<Class<? extends RecordTemplate>> currentBatch = new ArrayList<>(batchSize);

      for (Class<? extends RecordTemplate> recordClass : classes) {
        currentBatch.add(recordClass);
        if (currentBatch.size() >= batchSize) {
          // Submit batch and create fresh one
          List<Class<? extends RecordTemplate>> batchToSubmit = currentBatch;
          futures.add(executor.submit(() -> processBatch(batchToSubmit)));
          currentBatch = new ArrayList<>(batchSize);
        }
      }

      // Submit remaining batch
      if (!currentBatch.isEmpty()) {
        List<Class<? extends RecordTemplate>> batchToSubmit = currentBatch;
        futures.add(executor.submit(() -> processBatch(batchToSubmit)));
      }

      for (Future<?> future : futures) {
        // Worst case scenario
        future.get();
      }
    } catch (Exception e) {
      log.error("Error processing schemas in batches, falling back to sequential", e);
      // Fallback: process sequentially if batch processing fails
      for (Class recordClass : classes) {
        processClass(recordClass);
      }
    } finally {
      executor.shutdown();
    }
  }

  /** Process a batch of classes to extract schemas and register them. */
  private void processBatch(List<Class<? extends RecordTemplate>> batch) {
    for (Class<? extends RecordTemplate> recordClass : batch) {
      processClass(recordClass);
    }
  }

  /** Process a single class to extract schema and register as entity/aspect/event. */
  private void processClass(Class recordClass) {
    DataSchema schema = null;
    try {
      schema = DataTemplateUtil.getSchema(recordClass);
    } catch (Exception e) {
      // Not all classes have schemas. Ok to skip the ones we don't find
      return;
    }

    if (schema != null) {
      Map<String, Object> properties = schema.getProperties();
      if (properties != null) {
        DataSchema finalSchema = schema;
        getName(properties, EntityAnnotation.ANNOTATION_NAME)
            .ifPresent(entityName -> entitySchemas.put(entityName, finalSchema));
        getName(properties, AspectAnnotation.ANNOTATION_NAME)
            .ifPresent(
                aspectName -> {
                  aspectSchemas.put(aspectName, finalSchema);
                  aspectClasses.put(aspectName, recordClass);
                });
        getName(properties, EventAnnotation.ANNOTATION_NAME)
            .ifPresent(eventName -> eventSchemas.put(eventName, finalSchema));
      }
    }
  }

  public Optional<DataSchema> getEntitySchema(String entityName) {
    return Optional.ofNullable(entitySchemas.get(entityName));
  }

  public Optional<DataSchema> getAspectSchema(String aspectName) {
    return Optional.ofNullable(aspectSchemas.get(aspectName));
  }

  public Optional<DataSchema> getEventSchema(String eventName) {
    return Optional.ofNullable(eventSchemas.get(eventName));
  }

  public Optional<Class> getAspectClass(String aspectName) {
    return Optional.ofNullable(aspectClasses.get(aspectName));
  }

  @VisibleForTesting
  public Map<String, DataSchema> getEntitySchemaMap() {
    return entitySchemas;
  }

  @VisibleForTesting
  public Map<String, DataSchema> getAspectSchemaMap() {
    return aspectSchemas;
  }

  @VisibleForTesting
  public Map<String, DataSchema> getEventSchemaMap() {
    return eventSchemas;
  }

  @VisibleForTesting
  public Map<String, Class> getAspectClassMap() {
    return aspectClasses;
  }

  public static DataSchemaFactory getInstance() {
    return getInstance(false);
  }

  public static DataSchemaFactory getInstance(boolean useOptimizedEntityLoading) {
    if (INSTANCE == null) {
      synchronized (DataSchemaFactory.class) {
        if (INSTANCE == null) {
          INSTANCE = new DataSchemaFactory(useOptimizedEntityLoading);
        }
      }
    }
    return INSTANCE;
  }
}

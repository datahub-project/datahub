package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.metadata.models.annotation.EventAnnotation;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.reflections.Reflections;

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

  private static final DataSchemaFactory INSTANCE = new DataSchemaFactory();
  private static final String[] DEFAULT_TOP_LEVEL_NAMESPACES =
      new String[] {"com", "org", "io", "datahub"};

  public DataSchemaFactory() {
    this(new String[] {"com.linkedin", "com.datahub"});
  }

  public DataSchemaFactory(String classPath) {
    this(new String[] {classPath});
  }

  public DataSchemaFactory(String[] classPaths) {
    this(classPaths, null);
  }

  /**
   * Construct a DataSchemaFactory with classes and schemas found under a specific folder. This will
   * only look for classes under the `com`, `org` or `datahub` top level namespaces.
   *
   * @param pluginLocation The location of the classes and schema files.
   */
  public static DataSchemaFactory withCustomClasspath(Path pluginLocation) throws IOException {
    if (pluginLocation == null) {
      // no custom classpath, just return the default factory
      return INSTANCE;
    }
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
    return new DataSchemaFactory(DEFAULT_TOP_LEVEL_NAMESPACES, classLoader);
  }

  /**
   * Construct a DataSchemaFactory with a custom class loader and a list of class namespaces to look
   * for entities and aspects.
   */
  public DataSchemaFactory(String[] classNamespaces, ClassLoader customClassLoader) {
    entitySchemas = new HashMap<>();
    aspectSchemas = new HashMap<>();
    eventSchemas = new HashMap<>();
    aspectClasses = new HashMap();

    ClassLoader standardClassLoader = null;
    if (customClassLoader == null) {
      customClassLoader = Thread.currentThread().getContextClassLoader();
    } else {
      standardClassLoader = Thread.currentThread().getContextClassLoader();
    }
    Set<Class<? extends RecordTemplate>> classes = new HashSet<>();
    for (String namespace : classNamespaces) {
      log.debug("Reflections scanning {} namespace", namespace);
      Reflections reflections = new Reflections(namespace, customClassLoader);
      classes.addAll(reflections.getSubTypesOf(RecordTemplate.class));
    }
    log.debug("Found a total of {} RecordTemplate classes", classes.size());

    if (standardClassLoader != null) {
      Set<Class<? extends RecordTemplate>> stdClasses = new HashSet<>();
      for (String namespace : classNamespaces) {
        Reflections reflections = new Reflections(namespace, standardClassLoader);
        stdClasses.addAll(reflections.getSubTypesOf(RecordTemplate.class));
      }
      log.debug(
          "Standard ClassLoader found a total of {} RecordTemplate classes", stdClasses.size());
      classes.removeAll(stdClasses);
      log.debug("Finally found a total of {} RecordTemplate classes to inspect", classes.size());
    }

    for (Class recordClass : classes) {
      DataSchema schema = null;
      try {
        schema = DataTemplateUtil.getSchema(recordClass);
      } catch (Exception e) {
        // Not all classes have schemas. Ok to skip the ones we don't find
      }

      if (schema != null) {
        DataSchema finalSchema = schema;
        getName(schema, EntityAnnotation.ANNOTATION_NAME)
            .ifPresent(entityName -> entitySchemas.put(entityName, finalSchema));
        getName(schema, AspectAnnotation.ANNOTATION_NAME)
            .ifPresent(
                aspectName -> {
                  aspectSchemas.put(aspectName, finalSchema);
                  aspectClasses.put(aspectName, recordClass);
                });
        getName(schema, EventAnnotation.ANNOTATION_NAME)
            .ifPresent(
                eventName -> {
                  eventSchemas.put(eventName, finalSchema);
                });
      }
    }
  }

  private Optional<String> getName(DataSchema dataSchema, String annotationName) {
    return Optional.ofNullable(dataSchema.getProperties().get(annotationName))
        .filter(obj -> Map.class.isAssignableFrom(obj.getClass()))
        .flatMap(obj -> Optional.ofNullable(((Map) obj).get(NAME_FIELD)).map(Object::toString));
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

  public static DataSchemaFactory getInstance() {
    return INSTANCE;
  }
}

package com.linkedin.gms.factory.custom;

import com.linkedin.metadata.changeprocessor.AspectScope;
import com.linkedin.metadata.changeprocessor.ChangeProcessor;
import com.linkedin.metadata.changeprocessor.ChangeStreamProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.SortedSet;

@Configuration
@Slf4j
public class ChangeProcessorFactory {

  @Value("${CUSTOM_JAR_PATH:#{null}}")
  private String customJarPath;

  @Value("${CUSTOM_CLASS_NAME:#{null}}")
  private String customClassName;

  @Nullable
  public ChangeStreamProcessor createInstance() {
    try {
      if (customJarPath == null || customClassName == null) {
        log.debug("Not loading custom defined behaviour");
        return null;
      }
      log.debug("Loading custom defined behaviour {} from {}", customClassName, customJarPath);
//      Class<?> clazz = getMethodInstance(customJarPath, customClassName);

      ChangeStreamProcessor changeStreamProcessor = new ChangeStreamProcessor();

//      changeStreamProcessor.registerProcessor();

      return changeStreamProcessor;
    } catch (Exception ex) {
      log.error("Failed to load custom defined behaviour {} from {}", customClassName, customJarPath);
      return null;
    }
  }

  private Map<String, SortedSet<ChangeProcessor>> getMethodInstance(String jarUrl, String className) throws Exception {
    URL url = (new File(jarUrl).toURI().toURL());
    URLClassLoader classLoader = new URLClassLoader(new URL[]{url});

    ServiceLoader<ChangeProcessor> serviceLoader = ServiceLoader.load(ChangeProcessor.class, classLoader);

    Map<String, SortedSet<ChangeProcessor>> changeProcessorMap = new HashMap<>();
    Iterator<ChangeProcessor> iterator = serviceLoader.iterator();

    while (iterator.hasNext()){
      ChangeProcessor changeProcessor = iterator.next();


      AspectScope annotation = changeProcessor.getClass().getAnnotation(AspectScope.class);
    }

    return changeProcessorMap;
  }
}

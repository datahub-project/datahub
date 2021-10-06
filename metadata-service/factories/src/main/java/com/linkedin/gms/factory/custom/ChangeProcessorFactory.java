package com.linkedin.gms.factory.custom;

import com.linkedin.metadata.changeprocessor.ChangeProcessor;
import com.linkedin.metadata.changeprocessor.ChangeProcessorScope;
import com.linkedin.metadata.changeprocessor.ChangeProcessorType;
import com.linkedin.metadata.changeprocessor.ChangeStreamProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ServiceLoader;


@Configuration
@Slf4j
public class ChangeProcessorFactory {

  @Value("${PROCESSOR_JAR_PATH:#{null}}")
  private String _processorJarPath;

  @Nullable
  public ChangeStreamProcessor createInstance() {
    try {
      if (_processorJarPath == null) {
        log.debug("Not loading custom defined behaviour");
        return new ChangeStreamProcessor();
      }
      return createChangeStreamProcessor(_processorJarPath);
    } catch (Exception ex) {
      log.error("Failed to load custom defined behaviour {}", _processorJarPath);
      return null;
    }
  }

  private ChangeStreamProcessor createChangeStreamProcessor(String jarUrl) throws Exception {
    ChangeStreamProcessor changeStreamProcessor = new ChangeStreamProcessor();
    URL url = (new File(jarUrl).toURI().toURL());
    URLClassLoader classLoader = new URLClassLoader(new URL[]{url}, Thread.currentThread().getContextClassLoader());
    ServiceLoader<ChangeProcessor> serviceLoader = ServiceLoader.load(ChangeProcessor.class, classLoader);
    int processorCount = 0;

    for (ChangeProcessor changeProcessor : serviceLoader) {
      ChangeProcessorScope annotation = changeProcessor.getClass().getAnnotation(ChangeProcessorScope.class);
      for (String entityAspect : annotation.entityAspectNames()) {

        String[] keys = entityAspect.split("/");
        String entityName = keys[0];
        String aspectName = keys[1];

        if (annotation.processorType() == ChangeProcessorType.PRE) {
          changeStreamProcessor.registerPreProcessor(entityName, aspectName, changeProcessor);
        } else {
          changeStreamProcessor.registerPostProcessor(entityName, aspectName, changeProcessor);
        }
        processorCount += 1;
      }
    }

    log.info("Successfully loaded {} processors", processorCount);

    return changeStreamProcessor;
  }
}

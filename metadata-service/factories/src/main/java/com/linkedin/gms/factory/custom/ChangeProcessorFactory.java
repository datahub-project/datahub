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

class Program{

  public static void main(String[] args){

    ChangeProcessorFactory changeProcessorFactory = new ChangeProcessorFactory();

    changeProcessorFactory.createInstance();

  }
}

@Configuration
@Slf4j
public class ChangeProcessorFactory {

  //TODO revert back to #{null}
  @Value("${CUSTOM_JAR_PATH:/Users/matthew/IdeaProjects/ResourceRegistryStateMachine/build/libs/ResourceRegistryStateMachine-1.0-SNAPSHOT.jar}")
  private String customJarPath;

  @Nullable
  public ChangeStreamProcessor createInstance() {
    customJarPath = "/Users/labuser/Documents/source/matthewc/build/libs/matthewc-1.0-SNAPSHOT.jar";
    log.info("CUSTOM JAR PATH SET " + customJarPath);
    try {
      if (customJarPath == null) {
        log.debug("Not loading custom defined behaviour");
        return new ChangeStreamProcessor();
      }
      return createChangeStreamProcessor(customJarPath);
    } catch (Exception ex) {
      log.error("Failed to load custom defined behaviour {}", customJarPath);
      return null;
    }
  }

  private ChangeStreamProcessor createChangeStreamProcessor(String jarUrl) throws Exception {
    ChangeStreamProcessor changeStreamProcessor = new ChangeStreamProcessor();
    System.out.println("CLASSPATH " + System.getProperties().get("java.class.path"));

    URL url = (new File(jarUrl).toURI().toURL());
    URLClassLoader classLoader = new URLClassLoader(new URL[]{url}, Thread.currentThread().getContextClassLoader());
    ServiceLoader<ChangeProcessor> serviceLoader = ServiceLoader.load(ChangeProcessor.class, classLoader);
    int processorCount = 0;

    for (ChangeProcessor changeProcessor : serviceLoader) {
      ChangeProcessorScope annotation = changeProcessor.getClass().getAnnotation(ChangeProcessorScope.class);
      for (String entityAspect : annotation.entityAspectNames()) {

        String[] keys = entityAspect.split(":");
        String entityName = keys[0];
        String aspectName = keys[1];

        if (annotation.processorType() == ChangeProcessorType.BEFORE) {
          changeStreamProcessor.addBeforeChangeProcessor(entityName, aspectName, changeProcessor);
        } else {
          changeStreamProcessor.addAfterChangeProcessor(entityName, aspectName, changeProcessor);
        }
        processorCount += 1;
      }
    }

    log.info("Successfully loaded {} processors", processorCount);

    return changeStreamProcessor;
  }
}

package com.linkedin.gms.factory.custom;

import com.linkedin.metadata.changeprocessor.AspectScope;
import com.linkedin.metadata.changeprocessor.ChangeProcessor;
import com.linkedin.metadata.changeprocessor.ChangeStreamProcessor;
import com.linkedin.util.Pair;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

@Configuration
@Slf4j
public class ChangeProcessorFactory {

  @Value("${CUSTOM_JAR_PATH:#{null}}")
  private String customJarPath;

  @Nullable
  public ChangeStreamProcessor createInstance() {
    try {
      ChangeStreamProcessor changeStreamProcessor = new ChangeStreamProcessor();

      if (customJarPath == null ) {
        log.debug("Not loading custom defined behaviour");
        return changeStreamProcessor;
      }

      List<Pair<String, ChangeProcessor>> changeProcessors = buildChangeProcessors(customJarPath);

      for (Pair<String, ChangeProcessor> processor : changeProcessors) {
        changeStreamProcessor.registerProcessor(processor.getKey(), processor.getValue());
      }

      return changeStreamProcessor;
    } catch (Exception ex) {
      log.error("Failed to load custom defined behaviour {}", customJarPath);
      return null;
    }
  }

  private List<Pair<String, ChangeProcessor>> buildChangeProcessors(String jarUrl) throws Exception {
    URL url = (new File(jarUrl).toURI().toURL());
    URLClassLoader classLoader = new URLClassLoader(new URL[]{url});
    ServiceLoader<ChangeProcessor> serviceLoader = ServiceLoader.load(ChangeProcessor.class, classLoader);

    List<Pair<String, ChangeProcessor>> changeProcessors = new ArrayList<>();

    for (ChangeProcessor changeProcessor : serviceLoader) {
      String[] aspectNames = changeProcessor.getClass().getAnnotation(AspectScope.class).aspectNames();
      for (String aspectName : aspectNames) {
        changeProcessors.add(new Pair(aspectName, changeProcessor));
      }
    }

    return changeProcessors;
  }
}

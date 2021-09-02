package com.linkedin.gms.factory.custom;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.entity.ebean.BeforeUpdateLambda;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Optional;
import java.util.function.BiFunction;

@Configuration
@Slf4j
public class BeforeUpdateLambdaFactory {

  @Value("${CUSTOM_JAR_PATH:#{null}}")
  private String customJarPath;

  @Value("${CUSTOM_CLASS_NAME:#{null}}")
  private String customClassName;

  @Nullable
  public BeforeUpdateLambda createInstance() {
    try {
      if (customJarPath == null || customClassName == null) {
        log.debug("Not loading custom defined behaviour");
        return null;
      }
      log.debug("Loading custom defined behaviour {} from {}", customClassName, customJarPath);
      Class<?> clazz = getMethodInstance(customJarPath, customClassName);

      BiFunction<Optional<RecordTemplate>, RecordTemplate, Optional<RecordTemplate>> lambda =
          (BiFunction<Optional<RecordTemplate>, RecordTemplate, Optional<RecordTemplate>>) clazz.getDeclaredConstructor().newInstance();

      return new BeforeUpdateLambda(lambda);
    } catch (Exception ex) {
      log.error("Failed to load custom defined behaviour {} from {}", customClassName, customJarPath);
      return null;
    }
  }

  private Class<?> getMethodInstance(String jarUrl, String className) throws Exception {
    URL url = (new File(jarUrl).toURI().toURL());
    URLClassLoader classLoader = new URLClassLoader(new URL[]{url});
    return classLoader.loadClass(className);
  }
}

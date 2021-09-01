package com.linkedin.gms.factory.custom;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.entity.BeforeUpdateFunction;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

public class BeforeUpdateFunctionFactory {
  public BeforeUpdateFunction createInstance() {
    try {
      //Todo pull strings into config
      String jarUrl = "/Users/labuser/Documents/source/matthewc/build/libs/matthewc-1.0-SNAPSHOT.jar";
      String className = "com.matthewc.Target";
      String methodName = "apply";

      Class<?> clazz = getMethodInstance(jarUrl, className);
      Object instance = clazz.getDeclaredConstructor().newInstance();
      Method method = clazz.getMethod(methodName, int.class, int.class);

      // Parses into a function that accepts a Pegasus RecordTemplate
      return getBeforeUpdateFunction(instance, method);
    } catch (Exception ex) {
      return null;
    }
  }

  private BeforeUpdateFunction getBeforeUpdateFunction(Object instance, Method method) {
    return (oldRecordOption, newRecord) -> {
      try {
        return (RecordTemplate) method.invoke(instance, oldRecordOption, newRecord);
      } catch (Exception e) {
        return newRecord;
      }
    };
  }

  private Class<?> getMethodInstance(String jarUrl, String className) throws Exception {
    URL url = (new File(jarUrl).toURI().toURL());
    URLClassLoader classLoader = new URLClassLoader(new URL[]{url});
    return classLoader.loadClass(className);
  }
}

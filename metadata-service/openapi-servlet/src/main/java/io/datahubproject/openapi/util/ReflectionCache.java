package io.datahubproject.openapi.util;

import com.google.common.reflect.ClassPath;
import com.linkedin.util.Pair;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
public class ReflectionCache {
  private static final ConcurrentHashMap<String, Method> METHOD_CACHE = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();

  private final String basePackage;
  private final Set<String> subPackages;
  @Builder.Default // appropriate for lombok
  private final Function<Class<?>, String> getBuilderName =
      clazz -> String.join("", clazz.getSimpleName(), "$", clazz.getSimpleName(), "Builder");

  public static class ReflectionCacheBuilder {
    public ReflectionCacheBuilder basePackage(String basePackage) {
      return basePackage(basePackage, Set.of());
    }

    public ReflectionCacheBuilder basePackage(String basePackage, Set<String> packageExclusions) {
      this.basePackage = basePackage;
      return subPackages(
          findSubPackages(basePackage, Optional.ofNullable(packageExclusions).orElse(Set.of())));
    }

    private ReflectionCacheBuilder subPackages(Set<String> subPackages) {
      this.subPackages = subPackages;
      return this;
    }

    private Set<String> findSubPackages(String packageName, Set<String> exclusions) {
      try {
        return ClassPath.from(getClass().getClassLoader()).getAllClasses().stream()
            .filter(
                clazz ->
                    exclusions.stream().noneMatch(excl -> clazz.getPackageName().startsWith(excl))
                        && !clazz.getName().contains("$")
                        && clazz.getName().startsWith(packageName))
            .map(ClassPath.ClassInfo::getPackageName)
            .collect(Collectors.toSet());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public Method lookupMethod(Class<?> clazz, String method, Class<?>... parameters) {
    if (clazz == null) {
      return null;
    } else {
      return METHOD_CACHE.computeIfAbsent(
          String.join("_", clazz.getName(), method),
          key -> {
            try {
              log.debug(
                  "Lookup: "
                      + clazz.getName()
                      + " Method: "
                      + method
                      + " Parameters: "
                      + Arrays.toString(parameters));
              return clazz.getDeclaredMethod(method, parameters);
            } catch (NoSuchMethodException e) {
              return null;
            }
          });
    }
  }

  public Class<?> lookupClass(String className, boolean searchSubclass) {
    if (!searchSubclass) {
      return lookupClass(className);
    } else {
      List<String> subclasses = new LinkedList<>();
      subclasses.add(basePackage);
      if (subPackages != null) {
        subclasses.addAll(subPackages);
      }

      for (String packageName : subclasses) {
        try {
          return cachedClassLookup(packageName, className);
        } catch (Exception e) {
          log.debug("Class not found {}.{} ... continuing search", packageName, className);
        }
      }
    }
    throw new ClassCastException(
        String.format("Could not locate %s in package %s", className, basePackage));
  }

  public Class<?> lookupClass(String className) {
    return cachedClassLookup(basePackage, className);
  }

  private Class<?> cachedClassLookup(String packageName, String className) {
    return CLASS_CACHE.computeIfAbsent(
        String.format("%s.%s", packageName, className),
        key -> {
          try {
            log.debug("Lookup: " + key);
            return Class.forName(key);
          } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /** Get builder instance and class */
  public Pair<Class<?>, Object> getBuilder(Class<?> toClazz)
      throws InvocationTargetException, IllegalAccessException {
    Class<?> toClazzBuilder = lookupClass(getBuilderName.apply(toClazz));
    return Pair.of(toClazzBuilder, lookupMethod(toClazz, "builder").invoke(null));
  }

  public Method lookupMethod(
      Pair<Class<?>, Object> builderPair, String method, Class<?>... parameters) {
    return lookupMethod(builderPair.getFirst(), method, parameters);
  }

  /**
   * Convert class name to the pdl model names. Upper case first letter unless the 3rd character is
   * upper case. Reverse of {link ReflectionCache.toUpperFirst} i.e. MLModel -> mlModel Dataset ->
   * dataset DataProduct -> dataProduct
   *
   * @param s input string
   * @return class name
   */
  public static String toLowerFirst(String s) {
    if (s.length() > 2 && s.substring(2, 3).equals(s.substring(2, 3).toUpperCase())) {
      return s.substring(0, 2).toLowerCase() + s.substring(2);
    } else {
      return s.substring(0, 1).toLowerCase() + s.substring(1);
    }
  }

  /**
   * Convert the pdl model names to desired class names. Upper case first letter unless the 3rd
   * character is upper case. i.e. mlModel -> MLModel dataset -> Dataset dataProduct -> DataProduct
   *
   * @param s input string
   * @return class name
   */
  public static String toUpperFirst(String s) {
    if (s.length() > 2 && s.substring(2, 3).equals(s.substring(2, 3).toUpperCase())) {
      return s.substring(0, 2).toUpperCase() + s.substring(2);
    } else {
      return s.substring(0, 1).toUpperCase() + s.substring(1);
    }
  }
}

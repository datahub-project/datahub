package io.datahubproject.test.boot;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.util.ClassUtils;

/**
 * The beans a booted process must and must not contain. All violations are collected and reported
 * together, so one run shows the whole wiring diff rather than the first missing bean.
 *
 * <p>Types can be given as a {@link Class} or, for classes that are not on the test's compile
 * classpath (runtime-only modules), as a fully-qualified name. An unresolvable required type is a
 * violation; an unresolvable forbidden type is trivially absent.
 */
public final class BootContract {

  private final String process;
  private final List<BiConsumer<ApplicationContext, List<String>>> checks = new ArrayList<>();

  private BootContract(String process) {
    this.process = process;
  }

  public static BootContract forProcess(@Nonnull String process) {
    return new BootContract(process);
  }

  /** At least one bean assignable to each type. */
  public BootContract requires(@Nonnull Class<?>... types) {
    for (Class<?> type : types) {
      checks.add((ctx, v) -> requireType(ctx, v, type));
    }
    return this;
  }

  /** At least one bean assignable to each fully-qualified type name. */
  public BootContract requiresTypeNamed(@Nonnull String... typeNames) {
    for (String name : typeNames) {
      checks.add(
          (ctx, v) -> {
            Class<?> type = resolve(ctx, name);
            if (type == null) {
              v.add("required type not on classpath: " + name);
            } else {
              requireType(ctx, v, type);
            }
          });
    }
    return this;
  }

  /** A bean with this name, assignable to {@code type}. */
  public BootContract requiresBean(@Nonnull String beanName, @Nonnull Class<?> type) {
    checks.add(
        (ctx, v) -> {
          if (!ctx.containsBean(beanName)) {
            v.add("missing required bean '" + beanName + "' (" + type.getName() + ")");
            return;
          }
          Object bean = ctx.getBean(beanName);
          if (!type.isInstance(bean)) {
            v.add(
                "bean '"
                    + beanName
                    + "' is "
                    + bean.getClass().getName()
                    + ", expected "
                    + type.getName());
          }
        });
    return this;
  }

  public BootContract requiresBeans(@Nonnull String... beanNames) {
    for (String name : beanNames) {
      checks.add(
          (ctx, v) -> {
            if (!ctx.containsBean(name)) {
              v.add("missing required bean '" + name + "'");
            }
          });
    }
    return this;
  }

  /** No bean assignable to any of these types. */
  public BootContract forbids(@Nonnull Class<?>... types) {
    for (Class<?> type : types) {
      checks.add((ctx, v) -> forbidType(ctx, v, type));
    }
    return this;
  }

  public BootContract forbidsTypeNamed(@Nonnull String... typeNames) {
    for (String name : typeNames) {
      checks.add(
          (ctx, v) -> {
            Class<?> type = resolve(ctx, name);
            if (type != null) {
              forbidType(ctx, v, type);
            }
          });
    }
    return this;
  }

  public BootContract forbidsBeans(@Nonnull String... beanNames) {
    for (String name : beanNames) {
      checks.add(
          (ctx, v) -> {
            if (ctx.containsBean(name)) {
              v.add("forbidden bean '" + name + "' present (" + safeType(ctx, name) + ")");
            }
          });
    }
    return this;
  }

  /** Arbitrary check; add a message to the list for each violation. */
  public BootContract check(@Nonnull BiConsumer<ApplicationContext, List<String>> check) {
    checks.add(check);
    return this;
  }

  /** Throws {@link AssertionError} listing every violation. */
  public void verify(@Nonnull ApplicationContext ctx) {
    dumpIfRequested(ctx);
    List<String> violations = new ArrayList<>();
    for (BiConsumer<ApplicationContext, List<String>> check : checks) {
      check.accept(ctx, violations);
    }
    if (!violations.isEmpty()) {
      throw new AssertionError(
          process
              + " boot contract violated ("
              + violations.size()
              + "):\n  - "
              + String.join("\n  - ", violations));
    }
  }

  /**
   * With {@code -DbootTest.dumpDir=<dir>} (or env {@code BOOT_TEST_DUMP_DIR}), writes every bean
   * name and type to {@code <dir>/<process>.beans}; diff two dumps to see what a change rewired.
   */
  private void dumpIfRequested(ApplicationContext ctx) {
    String dir = System.getProperty("bootTest.dumpDir", System.getenv("BOOT_TEST_DUMP_DIR"));
    if (dir == null || dir.isBlank()) {
      return;
    }
    String[] names = ctx.getBeanDefinitionNames();
    Arrays.sort(names);
    StringBuilder out = new StringBuilder();
    for (String name : names) {
      out.append(name).append('\t').append(safeType(ctx, name)).append('\n');
    }
    try {
      Path path = Path.of(dir, process.replaceAll("[^A-Za-z0-9._-]", "_") + ".beans");
      Files.createDirectories(path.getParent());
      Files.writeString(path, out.toString());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void requireType(ApplicationContext ctx, List<String> v, Class<?> type) {
    if (ctx.getBeanNamesForType(type, true, false).length == 0) {
      v.add("missing required bean of type " + type.getName());
    }
  }

  private static void forbidType(ApplicationContext ctx, List<String> v, Class<?> type) {
    String[] names = ctx.getBeanNamesForType(type, true, false);
    if (names.length > 0) {
      v.add("forbidden type " + type.getName() + " present as " + Arrays.toString(names));
    }
  }

  private static Class<?> resolve(ApplicationContext ctx, String name) {
    try {
      return ClassUtils.forName(name, ctx.getClassLoader());
    } catch (ClassNotFoundException | LinkageError e) {
      return null;
    }
  }

  private static String safeType(ApplicationContext ctx, String name) {
    try {
      Class<?> type = ctx.getType(name);
      return type == null ? "?" : type.getName();
    } catch (NoSuchBeanDefinitionException e) {
      return "?";
    }
  }
}

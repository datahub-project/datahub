package io.datahubproject.test.boot;

import com.linkedin.metadata.aspect.hooks.AspectMigrationMutator;
import com.linkedin.metadata.aspect.hooks.AspectMigrationMutatorChain;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.stereotype.Component;

/**
 * Every concrete {@link AspectMigrationMutator} {@code @Component} on the classpath must be a bean
 * of the process and, with {@code featureFlags.aspectMigrationMutatorEnabled=true}, a member of the
 * {@link AspectMigrationMutatorChain}. The expected set is discovered by scanning, so a new mutator
 * in a package the process does not component-scan fails here instead of being silently skipped
 * during upgrades.
 */
public final class AspectMigrationMutators {

  private static final String[] SCAN_ROOTS = {"com.linkedin", "com.datahub", "io.datahubproject"};

  private static volatile Set<String> discovered;

  private AspectMigrationMutators() {}

  /** Class names of every concrete {@code @Component} mutator on the classpath. */
  public static Set<String> onClasspath() {
    if (discovered == null) {
      ClassPathScanningCandidateComponentProvider scanner =
          new ClassPathScanningCandidateComponentProvider(false);
      scanner.addIncludeFilter(new AssignableTypeFilter(AspectMigrationMutator.class));
      Set<String> found = new TreeSet<>();
      for (String root : SCAN_ROOTS) {
        for (BeanDefinition bd : scanner.findCandidateComponents(root)) {
          if (bd instanceof AnnotatedBeanDefinition abd
              && abd.getMetadata().isAnnotated(Component.class.getName())) {
            found.add(bd.getBeanClassName());
          }
        }
      }
      discovered = found;
    }
    return discovered;
  }

  /**
   * @param inChain also require each mutator to be registered in the chain (the chain only takes
   *     mutators when {@code featureFlags.aspectMigrationMutatorEnabled} is on)
   */
  public static BiConsumer<ApplicationContext, List<String>> allRegistered(boolean inChain) {
    return (ctx, v) -> {
      Set<String> expected = onClasspath();
      if (expected.isEmpty()) {
        v.add("no AspectMigrationMutator components found on the classpath; scan is broken");
        return;
      }
      Set<String> beans =
          ctx.getBeansOfType(AspectMigrationMutator.class).values().stream()
              .map(b -> b.getClass().getName())
              .collect(Collectors.toSet());
      Set<String> missing = new TreeSet<>(expected);
      missing.removeAll(beans);
      if (!missing.isEmpty()) {
        v.add("AspectMigrationMutator components on the classpath but not beans: " + missing);
      }
      if (inChain) {
        AspectMigrationMutatorChain chain =
            ctx.getBeanProvider(AspectMigrationMutatorChain.class).getIfAvailable();
        if (chain == null) {
          v.add("no AspectMigrationMutatorChain bean");
          return;
        }
        Set<String> chained =
            chain.getChainByAspect().values().stream()
                .flatMap(List::stream)
                .map(m -> m.getClass().getName())
                .collect(Collectors.toSet());
        Set<String> notChained = new TreeSet<>(expected);
        notChained.removeAll(chained);
        if (!notChained.isEmpty()) {
          v.add("AspectMigrationMutators not in AspectMigrationMutatorChain: " + notChained);
        }
      }
    };
  }
}

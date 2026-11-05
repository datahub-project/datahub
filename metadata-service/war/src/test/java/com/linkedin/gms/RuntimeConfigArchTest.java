package com.linkedin.gms;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static org.testng.Assert.assertFalse;

import com.linkedin.metadata.config.resolver.ConfigKeyConstants;
import com.tngtech.archunit.core.domain.AccessTarget.CodeUnitAccessTarget;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.domain.JavaCodeUnitAccess;
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.domain.JavaFieldAccess;
import com.tngtech.archunit.core.domain.JavaParameter;
import com.tngtech.archunit.core.domain.properties.HasAnnotations;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.annotations.Test;

/**
 * A config read through {@code getConfig(ConfigKeyConstants.X, default)} anywhere in production
 * code is a runtime config: nothing may still bind it at boot via its getter, {@code @Value} or
 * {@code @ConditionalOnProperty}.
 */
public class RuntimeConfigArchTest {

  private static final String VALUE = "org.springframework.beans.factory.annotation.Value";
  private static final String CONDITIONAL =
      "org.springframework.boot.autoconfigure.condition.ConditionalOnProperty";
  private static final String LOMBOK_GENERATED = "lombok.Generated";

  private static final JavaClasses CLASSES =
      new ClassFileImporter()
          .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
          // Project code only: skip third-party jars and generated models, which do not read
          // config.
          .withImportOption(
              location ->
                  location.contains("/build/")
                      && !location.contains("/generated/")
                      && !location.contains("metadata-models")
                      && !location.contains("auth-api")
                      && !location.contains("DataTemplate"))
          .importPackages(
              "com.linkedin.metadata",
              "com.linkedin.datahub",
              "com.linkedin.gms",
              "com.datahub",
              "io.datahubproject");

  @Test
  public void runtimeConfigsAreNotBoundAtBoot() {
    List<JavaFieldAccess> reads = runtimeConfigReads();
    assertFalse(reads.isEmpty(), "no ConfigKeyConstants reference found in production code");

    Set<String> getters =
        reads.stream().map(r -> zeroArgTwin(r.getOrigin())).collect(Collectors.toSet());
    classes().should(notCallRuntimeConfigGetters(getters)).check(CLASSES);

    for (String key :
        reads.stream().map(RuntimeConfigArchTest::keyOf).collect(Collectors.toSet())) {
      classes().should(notBindAtBoot(key)).check(CLASSES);
    }
  }

  /** Every read of a ConfigKeyConstants field by production code outside the constants class. */
  private static List<JavaFieldAccess> runtimeConfigReads() {
    String constants = ConfigKeyConstants.class.getName();
    return CLASSES.stream()
        .filter(c -> !c.getName().startsWith(constants))
        .flatMap(c -> c.getFieldAccessesFromSelf().stream())
        .filter(a -> a.getAccessType() == JavaFieldAccess.AccessType.GET)
        .filter(a -> a.getTarget().getOwner().getName().startsWith(constants))
        .toList();
  }

  /** The yaml path the read constant holds, e.g. {@code featureFlags.metricsEnabled}. */
  private static String keyOf(JavaFieldAccess read) {
    try {
      return (String)
          read.getTarget().getOwner().reflect().getField(read.getTarget().getName()).get(null);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * {@code Owner#name} of the zero-arg method sharing the reader's name: a per-operation read is an
   * overload of its getter, so {@code isX(operation)} protects {@code isX()}.
   */
  private static String zeroArgTwin(JavaCodeUnit reader) {
    if (reader.getOwner().tryGetMethod(reader.getName()).isEmpty()) {
      throw new AssertionError(
          reader.getFullName()
              + " reads a runtime config but has no zero-arg "
              + reader.getName()
              + "() to protect; make the per-operation read an overload of the config's getter");
    }
    return reader.getOwner().getName() + "#" + reader.getName();
  }

  private static ArchCondition<JavaClass> notCallRuntimeConfigGetters(Set<String> getters) {
    return new ArchCondition<>("not call the zero-arg getter of a runtime config") {
      @Override
      public void check(JavaClass clazz, ConditionEvents events) {
        Stream.concat(
                clazz.getMethodCallsFromSelf().stream(),
                clazz.getMethodReferencesFromSelf().stream())
            .filter(access -> callsGetter(access, getters) && !isOwnLombokMethod(access))
            .forEach(
                access ->
                    events.add(
                        SimpleConditionEvent.violated(
                            access,
                            access.getDescription()
                                + "; read it through the operation context instead")));
      }
    };
  }

  private static boolean callsGetter(JavaCodeUnitAccess<?> access, Set<String> getters) {
    CodeUnitAccessTarget target = access.getTarget();
    return target.getRawParameterTypes().isEmpty()
        && getters.contains(target.getOwner().getName() + "#" + target.getName());
  }

  /** Lombok's own equals/hashCode/toString read every property through its getter. */
  private static boolean isOwnLombokMethod(JavaCodeUnitAccess<?> access) {
    JavaCodeUnit origin = access.getOrigin();
    return origin.getOwner().getName().equals(access.getTarget().getOwner().getName())
        && origin.isAnnotatedWith(LOMBOK_GENERATED);
  }

  private static ArchCondition<JavaClass> notBindAtBoot(String key) {
    return new ArchCondition<>("not bind " + key + " via @Value or @ConditionalOnProperty") {
      @Override
      public void check(JavaClass clazz, ConditionEvents events) {
        checkAnnotated(clazz, clazz, key, events);
        for (JavaField f : clazz.getFields()) {
          checkAnnotated(f, clazz, key, events);
        }
        for (JavaCodeUnit unit : clazz.getCodeUnits()) {
          checkAnnotated(unit, clazz, key, events);
          for (JavaParameter p : unit.getParameters()) {
            checkAnnotated(p, clazz, key, events);
          }
        }
      }
    };
  }

  /** Annotation attributes may be arrays (ConditionalOnProperty.value/name). */
  private static String text(Object attribute) {
    return attribute instanceof Object[]
        ? Arrays.toString((Object[]) attribute)
        : String.valueOf(attribute);
  }

  private static void checkAnnotated(
      HasAnnotations<?> member, JavaClass clazz, String key, ConditionEvents events) {
    for (String annotation : new String[] {VALUE, CONDITIONAL}) {
      Optional<String> attrs =
          member
              .tryGetAnnotationOfType(annotation)
              .map(a -> text(a.get("value").orElse("")) + " " + text(a.get("name").orElse("")));
      if (attrs.isPresent() && attrs.get().contains(key)) {
        events.add(
            SimpleConditionEvent.violated(
                clazz,
                clazz.getName()
                    + " binds runtime config "
                    + key
                    + " at boot via @"
                    + annotation.substring(annotation.lastIndexOf('.') + 1)));
      }
    }
  }
}

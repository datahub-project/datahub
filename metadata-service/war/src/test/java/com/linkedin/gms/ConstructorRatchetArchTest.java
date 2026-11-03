package com.linkedin.gms;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaConstructor;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.springframework.asm.ClassReader;
import org.springframework.asm.ClassVisitor;
import org.springframework.asm.MethodVisitor;
import org.springframework.asm.Opcodes;
import org.testng.annotations.Test;

/**
 * Ratchet on telescoping and null-forwarding constructors.
 *
 * <p>A class with several public constructors, or one whose constructors delegate with {@code
 * this(..., null, ...)}, lets a caller pick an overload that silently drops a dependency. That is
 * how a factory once built the search mappings with a {@code null} search configuration: the
 * factory called the short constructor, the tests built the class with the long one, and every
 * suite stayed green. Give such a class one constructor (or a builder) instead.
 *
 * <p>Scope: classes constructed by a {@code @Bean} method, and every class in metadata-io,
 * metadata-service/services and datahub-graphql-core, except exceptions, records and persistence
 * classes. Current violators are listed in {@code constructor-ratchet-baseline.txt}, which may only
 * shrink: a new violator fails, and so does a listed class that no longer violates (delete its
 * line).
 */
public class ConstructorRatchetArchTest {

  private static final String BEAN = "org.springframework.context.annotation.Bean";
  private static final String LOMBOK_GENERATED = "lombok.Generated";

  /** Persistence and generated classes, whose constructors follow their framework's contract. */
  private static final List<String> EXEMPT_ANNOTATIONS =
      List.of(
          "jakarta.persistence.Entity",
          "jakarta.persistence.Embeddable",
          "io.ebean.typequery.Generated");

  private static final String BASELINE = "constructor-ratchet-baseline.txt";

  /** Path fragments of the modules where every class is in scope. */
  private static final List<String> SCOPED_MODULES =
      List.of("/metadata-io/", "/metadata-service/services/", "/datahub-graphql-core/");

  private static final JavaClasses CLASSES =
      new ClassFileImporter()
          .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
          // Project code only; generated Pegasus models have their own constructor conventions.
          .withImportOption(
              location ->
                  location.contains("/build/")
                      && !location.contains("/generated/")
                      && !location.contains("metadata-models")
                      && !location.contains("DataTemplate"))
          .importPackages(
              "com.linkedin.metadata",
              "com.linkedin.datahub",
              "com.linkedin.gms",
              "com.datahub",
              "io.datahubproject");

  @Test
  public void noNewTelescopingOrNullForwardingConstructors() throws IOException {
    SortedMap<String, String> violations = violations();
    Set<String> baseline = baseline();

    // Canary: the rule must still see the class whose constructor ladder caused the incident.
    assertTrue(
        violations.containsKey(
            "com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder"),
        "V2MappingsBuilder is no longer flagged; if its constructors were collapsed, remove this"
            + " canary along with its baseline line");

    Set<String> added = new TreeSet<>(violations.keySet());
    added.removeAll(baseline);
    Set<String> fixed = new TreeSet<>(baseline);
    fixed.removeAll(violations.keySet());
    if (added.isEmpty() && fixed.isEmpty()) {
      return;
    }

    Path actual = Path.of("build", BASELINE);
    Files.createDirectories(actual.getParent());
    Files.write(actual, violations.keySet(), StandardCharsets.UTF_8);
    StringBuilder msg = new StringBuilder();
    if (!added.isEmpty()) {
      msg.append("New telescoping or null-forwarding constructors. Give each class a single")
          .append(" public constructor (or a builder) and no this(..., null, ...) delegation:\n");
      added.forEach(
          c -> msg.append("  ").append(c).append(": ").append(violations.get(c)).append('\n'));
    }
    if (!fixed.isEmpty()) {
      msg.append("No longer violating; delete these lines from src/test/resources/")
          .append(BASELINE)
          .append(":\n");
      fixed.forEach(c -> msg.append("  ").append(c).append('\n'));
    }
    msg.append("The current violator list is at ").append(actual.toAbsolutePath());
    fail(msg.toString());
  }

  /** Class name to reason, for every in-scope class that violates the rule. */
  private static SortedMap<String, String> violations() {
    Set<JavaClass> beanBuilt = beanBuiltClasses();
    SortedMap<String, String> violations = new TreeMap<>();
    for (JavaClass clazz : CLASSES) {
      if (!exempt(clazz) && (beanBuilt.contains(clazz) || inScopedModule(clazz))) {
        String reason = reason(clazz);
        if (reason != null) {
          violations.put(clazz.getName(), reason);
        }
      }
    }
    return violations;
  }

  /** Project classes a {@code @Bean} method constructs or declares as its return type. */
  private static Set<JavaClass> beanBuiltClasses() {
    Set<JavaClass> built = new HashSet<>();
    for (JavaClass clazz : CLASSES) {
      for (JavaMethod method : clazz.getMethods()) {
        if (method.isAnnotatedWith(BEAN)) {
          method.getConstructorCallsFromSelf().forEach(call -> built.add(call.getTargetOwner()));
          built.add(method.getRawReturnType());
        }
      }
    }
    built.removeIf(c -> !CLASSES.contain(c.getName()));
    return built;
  }

  /**
   * Exceptions conventionally offer (message), (message, cause) overloads; records, JPA/Ebean
   * entities and Ebean query beans have constructors dictated by their framework.
   */
  private static boolean exempt(JavaClass clazz) {
    if (clazz.getModifiers().contains(JavaModifier.SYNTHETIC)
        || clazz.isAnonymousClass()
        || clazz.isRecord()
        || clazz.isAssignableTo(Throwable.class)) {
      return true;
    }
    for (JavaClass c = clazz; c != null; c = c.getEnclosingClass().orElse(null)) {
      JavaClass current = c;
      if (EXEMPT_ANNOTATIONS.stream().anyMatch(current::isAnnotatedWith)) {
        return true;
      }
    }
    return false;
  }

  private static boolean inScopedModule(JavaClass clazz) {
    String location = clazz.getSource().map(s -> s.getUri().toString()).orElse("");
    return SCOPED_MODULES.stream().anyMatch(location::contains);
  }

  /**
   * Why the class violates the rule, or null. Several public constructors count only if at least
   * one is hand-written: a pure Lombok {@code @NoArgsConstructor} + {@code @AllArgsConstructor}
   * pair (Jackson/builder POJOs) cannot forward a default the caller did not ask for.
   */
  private static String reason(JavaClass clazz) {
    List<JavaConstructor> publicCtors =
        clazz.getConstructors().stream()
            .filter(c -> c.getModifiers().contains(JavaModifier.PUBLIC))
            .toList();
    if (publicCtors.stream().allMatch(c -> c.isAnnotatedWith(LOMBOK_GENERATED))) {
      publicCtors = publicCtors.subList(0, Math.min(1, publicCtors.size()));
    }
    Set<String> nullForwarding = nullForwardingConstructors(clazz);
    if (publicCtors.size() > 1 && !nullForwarding.isEmpty()) {
      return publicCtors.size() + " public constructors, this(..., null, ...) in " + nullForwarding;
    }
    if (publicCtors.size() > 1) {
      return publicCtors.size() + " public constructors";
    }
    if (!nullForwarding.isEmpty()) {
      return "this(..., null, ...) in " + nullForwarding;
    }
    return null;
  }

  /**
   * Descriptors of the constructors that pass a {@code null} literal to another constructor of the
   * same class. Arguments are evaluated before the {@code this(...)} call, so an {@code
   * ACONST_NULL} ahead of it is (nearly always) one of its arguments.
   */
  private static Set<String> nullForwardingConstructors(JavaClass clazz) {
    String internalName = clazz.getName().replace('.', '/');
    Set<String> result = new TreeSet<>();
    try (InputStream in =
        Objects.requireNonNull(
            ConstructorRatchetArchTest.class
                .getClassLoader()
                .getResourceAsStream(internalName + ".class"),
            clazz.getName())) {
      new ClassReader(in)
          .accept(
              new ClassVisitor(Opcodes.ASM9) {
                @Override
                public MethodVisitor visitMethod(
                    int access, String name, String desc, String sig, String[] exceptions) {
                  if (!name.equals("<init>")) {
                    return null;
                  }
                  return new MethodVisitor(Opcodes.ASM9) {
                    private boolean sawNull;
                    private boolean delegated;

                    @Override
                    public void visitInsn(int opcode) {
                      if (opcode == Opcodes.ACONST_NULL && !delegated) {
                        sawNull = true;
                      }
                    }

                    @Override
                    public void visitMethodInsn(
                        int opcode, String owner, String method, String d, boolean itf) {
                      if (!delegated
                          && opcode == Opcodes.INVOKESPECIAL
                          && method.equals("<init>")
                          && owner.equals(internalName)) {
                        delegated = true;
                        if (sawNull) {
                          result.add(desc);
                        }
                      }
                    }
                  };
                }
              },
              ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return result;
  }

  private static Set<String> baseline() throws IOException {
    try (InputStream in =
        Objects.requireNonNull(
            ConstructorRatchetArchTest.class.getClassLoader().getResourceAsStream(BASELINE),
            BASELINE)) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8)
          .lines()
          .map(String::trim)
          .filter(l -> !l.isEmpty() && !l.startsWith("#"))
          .collect(Collectors.toSet());
    }
  }
}

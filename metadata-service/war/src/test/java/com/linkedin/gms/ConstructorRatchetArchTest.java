package com.linkedin.gms;

import static org.testng.Assert.assertEquals;
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
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.VarInsnNode;
import org.objectweb.asm.tree.analysis.Analyzer;
import org.objectweb.asm.tree.analysis.AnalyzerException;
import org.objectweb.asm.tree.analysis.Frame;
import org.objectweb.asm.tree.analysis.SourceInterpreter;
import org.objectweb.asm.tree.analysis.SourceValue;
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
          // Project code only; generated Pegasus models and GraphQL types have their own
          // constructor conventions.
          .withImportOption(
              location ->
                  location.contains("/build/")
                      && !location.contains("/generated/")
                      && !location.contains("metadata-models")
                      && !location.contains("data-template"))
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

  @Test
  public void nullForwardingDetectionCountsOnlyNullsPassedStraightThrough() {
    assertEquals(nullForwardingFixture(DirectNull.class).size(), 1);
    assertEquals(nullForwardingFixture(CastNull.class).size(), 1);
    assertTrue(nullForwardingFixture(NullInsideNestedCall.class).isEmpty());
    assertTrue(nullForwardingFixture(NullInsideNewObject.class).isEmpty());
    assertEquals(nullForwardingFixture(NullBeforeNewObject.class).size(), 1);
    assertEquals(nullForwardingFixture(NullBeforeCall.class).size(), 1);
    assertEquals(nullForwardingFixture(NullInTernary.class).size(), 1);
  }

  private static Set<String> nullForwardingFixture(Class<?> fixture) {
    return nullForwardingConstructors(new ClassFileImporter().importClass(fixture));
  }

  static class DirectNull {
    DirectNull(String a, String b) {}

    DirectNull(String a) {
      this(a, null);
    }
  }

  static class CastNull {
    CastNull(Object a) {}

    CastNull() {
      this((String) null);
    }
  }

  static class NullInsideNestedCall {
    NullInsideNestedCall(String a) {}

    NullInsideNestedCall() {
      this(String.valueOf((Object) null));
    }
  }

  static class NullBeforeNewObject {
    NullBeforeNewObject(String a, String b, StringBuilder c) {}

    NullBeforeNewObject(String a) {
      this(a, null, new StringBuilder());
    }
  }

  static class NullBeforeCall {
    NullBeforeCall(String a, String b, List<String> c) {}

    NullBeforeCall(String a) {
      this(a, null, List.of());
    }
  }

  static class NullInTernary {
    NullInTernary(String a) {}

    NullInTernary(Object a) {
      this(a == null ? null : a.toString());
    }
  }

  static class NullInsideNewObject {
    NullInsideNewObject(StringBuilder a) {}

    NullInsideNewObject() {
      this(new StringBuilder((CharSequence) null));
    }
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
    if (isLombokNoArgsAllArgsPair(clazz, publicCtors)) {
      publicCtors = publicCtors.subList(0, 1);
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
   * Exactly a Lombok {@code @NoArgsConstructor} + {@code @AllArgsConstructor} pair: two generated
   * public constructors, one empty and one taking every instance field. Any other generated
   * combination (e.g. {@code @RequiredArgsConstructor} + {@code @AllArgsConstructor}) still counts.
   */
  private static boolean isLombokNoArgsAllArgsPair(JavaClass clazz, List<JavaConstructor> ctors) {
    if (ctors.size() != 2 || !ctors.stream().allMatch(c -> c.isAnnotatedWith(LOMBOK_GENERATED))) {
      return false;
    }
    long instanceFields =
        clazz.getFields().stream()
            .filter(f -> !f.getModifiers().contains(JavaModifier.STATIC))
            .filter(f -> !f.getModifiers().contains(JavaModifier.SYNTHETIC))
            .count();
    Set<Integer> arities =
        ctors.stream().map(c -> c.getRawParameterTypes().size()).collect(Collectors.toSet());
    return arities.size() == 2 && arities.contains(0) && arities.contains((int) instanceFields);
  }

  /**
   * Descriptors of the constructors that pass a {@code null} literal (possibly cast, possibly one
   * branch of a ternary) as an argument of {@code this(...)}. Runs ASM's data-flow analysis so each
   * argument is traced to the instructions that produced it: a null consumed inside an argument
   * expression (for example {@code this(Objects.requireNonNull(null))} or {@code this(new
   * Foo(null))}) does not count, wherever it sits among the other arguments.
   */
  private static Set<String> nullForwardingConstructors(JavaClass clazz) {
    String internalName = clazz.getName().replace('.', '/');
    ClassNode node = new ClassNode();
    try (InputStream in =
        Objects.requireNonNull(
            ConstructorRatchetArchTest.class
                .getClassLoader()
                .getResourceAsStream(internalName + ".class"),
            clazz.getName())) {
      new ClassReader(in).accept(node, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    Set<String> result = new TreeSet<>();
    for (MethodNode ctor : node.methods) {
      if (ctor.name.equals("<init>") && forwardsNull(internalName, ctor)) {
        result.add(ctor.desc);
      }
    }
    return result;
  }

  private static boolean forwardsNull(String owner, MethodNode ctor) {
    Frame<SourceValue>[] frames;
    try {
      frames = new Analyzer<>(new NullThroughCastInterpreter()).analyze(owner, ctor);
    } catch (AnalyzerException e) {
      throw new IllegalStateException(owner + "#" + ctor.desc, e);
    }
    for (int i = 0; i < ctor.instructions.size(); i++) {
      if (frames[i] != null
          && ctor.instructions.get(i) instanceof MethodInsnNode call
          && call.getOpcode() == Opcodes.INVOKESPECIAL
          && call.name.equals("<init>")
          && call.owner.equals(owner)) {
        Frame<SourceValue> frame = frames[i];
        int args = Type.getArgumentTypes(call.desc).length;
        int top = frame.getStackSize() - 1;
        SourceValue receiver = frame.getStack(top - args);
        boolean onThis =
            receiver.insns.stream()
                .anyMatch(
                    n ->
                        n instanceof VarInsnNode v && v.getOpcode() == Opcodes.ALOAD && v.var == 0);
        if (!onThis) {
          continue; // new Self(...) inside a constructor, not this(...)
        }
        for (int a = 0; a < args; a++) {
          if (frame.getStack(top - a).insns.stream()
              .anyMatch(n -> n.getOpcode() == Opcodes.ACONST_NULL)) {
            return true;
          }
        }
        return false;
      }
    }
    return false;
  }

  /** Lets a null literal's source survive a cast, so {@code this((Foo) null)} is still a null. */
  private static class NullThroughCastInterpreter extends SourceInterpreter {
    NullThroughCastInterpreter() {
      super(Opcodes.ASM9);
    }

    @Override
    public SourceValue unaryOperation(AbstractInsnNode insn, SourceValue value) {
      return insn.getOpcode() == Opcodes.CHECKCAST
          ? new SourceValue(value.size, value.insns)
          : super.unaryOperation(insn, value);
    }
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

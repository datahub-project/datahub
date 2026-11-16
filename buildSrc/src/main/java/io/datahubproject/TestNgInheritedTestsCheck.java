package io.datahubproject;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Finds TestNG test classes that Gradle's test detection silently skips.
 *
 * <p>Gradle's TestNG detector never reads method annotations on an abstract class: it only counts
 * a class-level {@code @Test} there. A concrete class whose only {@code @Test} methods are
 * inherited from an abstract base without a class-level {@code @Test} is therefore never handed
 * to TestNG, and the build stays green while the tests never run.
 *
 * <p>This check reproduces Gradle's detection rules (see Gradle's {@code
 * AbstractTestFrameworkDetector} and {@code TestNGTestClassDetector}) next to what TestNG itself
 * would run, and reports every class where the two disagree.
 */
public final class TestNgInheritedTestsCheck {
  private static final String TEST = "Lorg/testng/annotations/Test;";

  /** Method annotations that make Gradle treat a concrete class as a TestNG test class. */
  private static final Set<String> GRADLE_DETECTED_METHOD_ANNOTATIONS =
      Set.of(
          TEST,
          "Lorg/testng/annotations/BeforeSuite;",
          "Lorg/testng/annotations/AfterSuite;",
          "Lorg/testng/annotations/BeforeTest;",
          "Lorg/testng/annotations/AfterTest;",
          "Lorg/testng/annotations/BeforeGroups;",
          "Lorg/testng/annotations/AfterGroups;",
          "Lorg/testng/annotations/Factory;");

  private static final class ClassInfo {
    String name;
    String superName;
    boolean isAbstract;
    boolean classLevelTest;
    boolean hasTestMethod;
    boolean hasGradleDetectedMethod;
  }

  private final URLClassLoader loader;
  private final Map<String, ClassInfo> cache = new HashMap<>();

  private TestNgInheritedTestsCheck(Iterable<File> classpath) {
    List<URL> urls = new ArrayList<>();
    for (File f : classpath) {
      try {
        urls.add(f.toURI().toURL());
      } catch (MalformedURLException e) {
        throw new IllegalStateException(e);
      }
    }
    this.loader = new URLClassLoader(urls.toArray(new URL[0]), null);
  }

  /**
   * Returns one message per concrete class in {@code candidateClassFiles} that TestNG would run but
   * Gradle's test detection skips.
   *
   * @param candidateClassFiles the test task's candidate class files
   * @param classpath test class dirs plus the test runtime classpath, to resolve superclasses
   */
  public static List<String> findUndetectedTestClasses(
      Iterable<File> candidateClassFiles, Iterable<File> classpath) {
    TestNgInheritedTestsCheck check = new TestNgInheritedTestsCheck(classpath);
    try {
      List<String> problems = new ArrayList<>();
      for (File file : candidateClassFiles) {
        if (!file.getName().endsWith(".class")) {
          continue;
        }
        ClassInfo info;
        try (InputStream in = Files.newInputStream(file.toPath())) {
          info = read(in);
        }
        if (info == null || info.isAbstract || check.gradleDetects(info)) {
          continue;
        }
        String testSource = check.inheritedTestSource(info);
        if (testSource != null) {
          problems.add(
              info.name.replace('/', '.')
                  + " inherits TestNG tests from abstract "
                  + testSource.replace('/', '.')
                  + ", but Gradle does not detect it as a test class, so it never runs."
                  + " Add a class-level @Test to "
                  + testSource.replace('/', '.')
                  + " (or, if you cannot change it, to "
                  + info.name.replace('/', '.')
                  + ").");
        }
      }
      return problems;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      try {
        check.loader.close();
      } catch (IOException ignored) {
        // best effort
      }
    }
  }

  /** Gradle's rule: walk up the superclasses; abstract classes only count with class-level @Test. */
  private boolean gradleDetects(ClassInfo info) {
    for (ClassInfo c = info; c != null; c = lookup(c.superName)) {
      if (c.classLevelTest || (!c.isAbstract && c.hasGradleDetectedMethod)) {
        return true;
      }
    }
    return false;
  }

  /**
   * TestNG's rule: any {@code @Test}, class- or method-level, anywhere in the hierarchy. Returns
   * the abstract class the tests come from (the one Gradle skipped), or null if TestNG would run
   * nothing.
   */
  private String inheritedTestSource(ClassInfo info) {
    for (ClassInfo c = info; c != null; c = lookup(c.superName)) {
      if (c.classLevelTest || c.hasTestMethod) {
        return c.name;
      }
    }
    return null;
  }

  private ClassInfo lookup(String internalName) {
    if (internalName == null || internalName.equals("java/lang/Object")) {
      return null;
    }
    return cache.computeIfAbsent(
        internalName,
        n -> {
          try (InputStream in = loader.getResourceAsStream(n + ".class")) {
            return in == null ? null : read(in);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }

  private static ClassInfo read(InputStream in) throws IOException {
    ClassInfo info = new ClassInfo();
    byte[] bytes = in.readAllBytes();
    try {
      parse(new ClassReader(bytes), info);
    } catch (RuntimeException e) {
      // Gradle treats a class file it cannot parse as a test class, so there is nothing to report.
      return null;
    }
    return info;
  }

  private static void parse(ClassReader reader, ClassInfo info) {
    reader.accept(
        new ClassVisitor(Opcodes.ASM9) {
          @Override
          public void visit(
              int version,
              int access,
              String name,
              String signature,
              String superName,
              String[] interfaces) {
            info.name = name;
            info.superName = superName;
            info.isAbstract = (access & (Opcodes.ACC_ABSTRACT | Opcodes.ACC_INTERFACE)) != 0;
          }

          @Override
          public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
            if (TEST.equals(desc)) {
              info.classLevelTest = true;
            }
            return null;
          }

          @Override
          public MethodVisitor visitMethod(
              int access, String name, String desc, String signature, String[] exceptions) {
            return new MethodVisitor(Opcodes.ASM9) {
              @Override
              public AnnotationVisitor visitAnnotation(String annotationDesc, boolean visible) {
                if (TEST.equals(annotationDesc)) {
                  info.hasTestMethod = true;
                }
                if (GRADLE_DETECTED_METHOD_ANNOTATIONS.contains(annotationDesc)) {
                  info.hasGradleDetectedMethod = true;
                }
                return null;
              }
            };
          }
        },
        ClassReader.SKIP_CODE | ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
  }
}

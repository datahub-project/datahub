package io.datahubproject.wiring;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.IgnoreEmptyDirectories;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Wiring ratchet: every Spring configuration class in a module's main output must be really
 * instantiated by at least one of the module's Spring tests, or be listed in the module's {@code
 * wiring-baseline.txt}, which may only shrink. See {@code gradle/wiring/README.md}.
 *
 * <p>A <em>configuration class</em> is a concrete class annotated with {@code @Configuration}
 * (directly or through {@code @SpringBootApplication} and friends) or one that declares {@code @Bean}
 * methods. Annotations meta-annotated with {@code @Configuration} are recognised when they are one
 * of the well-known Spring ones or are declared in the same module.
 *
 * <p>It counts as covered when a test context invoked one of its own (not inherited) {@code @Bean}
 * methods for real (a {@code @MockitoBean} replacement does not count), or, for classes without
 * {@code @Bean} methods, when a test context instantiated it. See {@link WiringCoverageEvaluator}.
 */
@CacheableTask
public abstract class WiringCoverageCheck extends DefaultTask {

  private static final Set<String> CONFIG_ANNOTATIONS =
      Set.of(
          "Lorg/springframework/context/annotation/Configuration;",
          "Lorg/springframework/boot/SpringBootConfiguration;",
          "Lorg/springframework/boot/autoconfigure/SpringBootApplication;",
          "Lorg/springframework/boot/autoconfigure/AutoConfiguration;",
          "Lorg/springframework/boot/test/context/TestConfiguration;");
  private static final String BEAN_ANNOTATION = "Lorg/springframework/context/annotation/Bean;";

  /** Compiled main classes of the module. */
  @InputFiles
  @PathSensitive(PathSensitivity.RELATIVE)
  @IgnoreEmptyDirectories
  public abstract ConfigurableFileCollection getClassesDirs();

  /**
   * Files written by the test-side recorder ({@code :wiring-coverage}). Their names are random per
   * test JVM, so only contents are fingerprinted.
   */
  @InputFiles
  @PathSensitive(PathSensitivity.NONE)
  public abstract ConfigurableFileCollection getRecordings();

  /** The checked-in baseline. Read through {@link #getBaselineContents()} so it may be absent. */
  @Internal
  public abstract RegularFileProperty getBaseline();

  @Input
  public String getBaselineContents() throws IOException {
    File f = getBaseline().get().getAsFile();
    return f.isFile() ? Files.readString(f.toPath()) : "";
  }

  @Input
  public abstract Property<String> getProjectPath();

  /** Only used to print paths relative to the repository root. */
  @Internal
  public abstract DirectoryProperty getRootDirectory();

  /** {@code -PupdateWiringBaseline}: prune covered/removed classes from the baseline. */
  @Input
  public abstract Property<Boolean> getUpdateBaseline();

  @OutputFile
  public abstract RegularFileProperty getReport();

  @TaskAction
  public void check() throws IOException {
    Map<String, Set<String>> targets = scanConfigurationClasses();
    Set<String> recorded = readRecordings();

    File baselineFile = getBaseline().get().getAsFile();
    List<String> baselineLines =
        baselineFile.isFile()
            ? Files.readAllLines(baselineFile.toPath(), StandardCharsets.UTF_8)
            : List.of();
    WiringCoverageEvaluator.Result result =
        WiringCoverageEvaluator.evaluate(
            targets, recorded, WiringCoverageEvaluator.parseBaseline(baselineLines));
    Set<String> covered = result.covered();
    Set<String> uncovered = result.uncovered();
    Set<String> newUncovered = result.newUncovered();
    Set<String> stale = new TreeSet<>(result.stale());

    writeReport(targets.size(), covered, uncovered, result.uncalledBeanMethods());

    if (getUpdateBaseline().get() && !stale.isEmpty()) {
      Files.write(
          baselineFile.toPath(),
          WiringCoverageEvaluator.prune(baselineLines, stale),
          StandardCharsets.UTF_8);
      getLogger()
          .lifecycle(
              "Wiring baseline {}: removed {} entries no longer needed.",
              rootRelative(baselineFile),
              stale.size());
      stale.clear();
    }

    getLogger()
        .info(
            "Wiring coverage {}: {}/{} configuration classes loaded by a real-context test, {}"
                + " baselined.",
            getProjectPath().get(),
            covered.size(),
            targets.size(),
            uncovered.size());

    if (newUncovered.isEmpty() && stale.isEmpty()) {
      return;
    }
    StringBuilder msg = new StringBuilder();
    msg.append("Wiring coverage check failed for ").append(getProjectPath().get()).append(".\n");
    if (!newUncovered.isEmpty()) {
      msg.append(
          "\nThese Spring configuration classes are not loaded by any real-context test in this"
              + " module:\n");
      newUncovered.forEach(c -> msg.append("  ").append(c).append('\n'));
      msg.append(
          "\nAdd a Spring test (@ContextConfiguration / AbstractTestNGSpringContextTests /"
              + " @SpringBootTest, or ApplicationContextRunner with"
              + " .withInitializer(WiringCoverage.initializer())) that imports the class and lets"
              + " at least one of its @Bean methods run for real, i.e. not replaced by"
              + " @MockitoBean. Mock its dependencies instead.\n"
              + "Adding the class to "
              + rootRelative(baselineFile)
              + " is a last resort for reviewers to judge; that file is meant to shrink only.\n");
    }
    if (!stale.isEmpty()) {
      msg.append(
          "\nThese classes are listed in "
              + rootRelative(baselineFile)
              + " but are now covered by a real-context test (or no longer exist). Remove them so"
              + " the baseline keeps shrinking:\n");
      stale.forEach(c -> msg.append("  ").append(c).append('\n'));
      msg.append(
          "\nOr run: ./gradlew "
              + taskPrefix()
              + "test -PupdateWiringBaseline (prunes only, never adds).\n");
    }
    msg.append("\nReport: ").append(rootRelative(getReport().get().getAsFile()));
    throw new GradleException(msg.toString());
  }

  private String taskPrefix() {
    String p = getProjectPath().get();
    return ":".equals(p) ? ":" : p + ":";
  }

  private String rootRelative(File f) {
    return getRootDirectory().get().getAsFile().toPath().relativize(f.toPath()).toString();
  }

  private void writeReport(
      int total,
      Set<String> covered,
      Set<String> uncovered,
      Map<String, Set<String>> uncalledBeanMethods)
      throws IOException {
    List<String> out = new ArrayList<>();
    out.add("# Wiring coverage for " + getProjectPath().get());
    out.add(
        "# "
            + covered.size()
            + "/"
            + total
            + " configuration classes loaded by a real-context test");
    out.add("");
    out.add("## Covered");
    out.addAll(covered);
    out.add("");
    out.add("## Not covered");
    out.addAll(uncovered);
    out.add("");
    out.add("## Covered classes with @Bean methods no test invoked (informational)");
    uncalledBeanMethods.forEach((c, ms) -> ms.forEach(m -> out.add(c + "#" + m)));
    File report = getReport().get().getAsFile();
    Files.createDirectories(report.getParentFile().toPath());
    Files.write(report.toPath(), out, StandardCharsets.UTF_8);
  }

  private Set<String> readRecordings() throws IOException {
    Set<String> lines = new HashSet<>();
    for (File f : getRecordings().getAsFileTree().getFiles()) {
      if (f.getName().endsWith(".txt")) {
        lines.addAll(Files.readAllLines(f.toPath(), StandardCharsets.UTF_8));
      }
    }
    return lines;
  }

  /** Configuration class name (binary, {@code $} for nested) to its own {@code @Bean} methods. */
  private Map<String, Set<String>> scanConfigurationClasses() {
    List<ClassInfo> classes = new ArrayList<>();
    for (File f : getClassesDirs().getAsFileTree().getFiles()) {
      if (f.getName().endsWith(".class")
          && !f.getName().equals("module-info.class")
          && !f.getName().equals("package-info.class")) {
        classes.add(read(f));
      }
    }
    // Annotations declared in this module that are themselves meta-annotated with @Configuration.
    Set<String> configAnnotations = new HashSet<>(CONFIG_ANNOTATIONS);
    boolean grew = true;
    while (grew) {
      grew = false;
      for (ClassInfo c : classes) {
        if ((c.access & Opcodes.ACC_ANNOTATION) != 0
            && !configAnnotations.contains(c.descriptor())
            && c.annotations.stream().anyMatch(configAnnotations::contains)) {
          configAnnotations.add(c.descriptor());
          grew = true;
        }
      }
    }
    Map<String, Set<String>> targets = new TreeMap<>();
    for (ClassInfo c : classes) {
      if ((c.access & (Opcodes.ACC_INTERFACE | Opcodes.ACC_ABSTRACT | Opcodes.ACC_SYNTHETIC))
          != 0) {
        continue;
      }
      boolean config = c.annotations.stream().anyMatch(configAnnotations::contains);
      if (config || !c.beanMethods.isEmpty()) {
        targets.put(c.internalName.replace('/', '.'), c.beanMethods);
      }
    }
    return targets;
  }

  private static ClassInfo read(File f) {
    try (InputStream in = Files.newInputStream(f.toPath())) {
      ClassInfo info = new ClassInfo();
      new ClassReader(in)
          .accept(
              new ClassVisitor(Opcodes.ASM9) {
                @Override
                public void visit(
                    int version,
                    int access,
                    String name,
                    String signature,
                    String superName,
                    String[] interfaces) {
                  info.access = access;
                  info.internalName = name;
                }

                @Override
                public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
                  info.annotations.add(descriptor);
                  return null;
                }

                @Override
                public MethodVisitor visitMethod(
                    int access,
                    String name,
                    String descriptor,
                    String signature,
                    String[] exceptions) {
                  return new MethodVisitor(Opcodes.ASM9) {
                    @Override
                    public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
                      if (BEAN_ANNOTATION.equals(desc)) {
                        info.beanMethods.add(name);
                      }
                      return null;
                    }
                  };
                }
              },
              ClassReader.SKIP_CODE | ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
      return info;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static final class ClassInfo {
    int access;
    String internalName;
    final Set<String> annotations = new HashSet<>();
    final Set<String> beanMethods = new TreeSet<>();

    String descriptor() {
      return "L" + internalName + ";";
    }
  }
}

package io.datahubproject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Task;
import org.gradle.api.tasks.testing.Test;
import org.gradle.api.tasks.testing.testng.TestNGOptions;

/**
 * Fails a TestNG-run {@link Test} task whose test classes carry a JUnit 4/5 test annotation.
 *
 * <p>TestNG silently skips JUnit test methods: the class compiles, the task stays green, and its
 * tests never execute. The checked descriptors appear in a class file only where that annotation
 * is applied (or a field/parameter has that annotation type), so helpers that merely depend on
 * JUnit are not flagged. Not detected: custom meta-annotations built on {@code @Test}, JUnit
 * 5.13+ {@code @ParameterizedClass}/{@code @ClassTemplate}, and JUnit 3 {@code TestCase}
 * subclasses; none are used in TestNG modules.
 *
 * <p>Only this direction is checked: TestNG classes in a JUnit-run task are not detected.
 *
 * <p>A buildSrc class rather than a build-script closure, so the Test tasks' build-cache keys do
 * not change with every edit to the root build script.
 */
public class JUnitInTestNGGuard implements Action<Task> {

  private static final List<String> JUNIT_TEST_ANNOTATIONS =
      List.of(
          "Lorg/junit/Test;",
          "Lorg/junit/experimental/theories/Theory;",
          "Lorg/junit/jupiter/api/Test;",
          "Lorg/junit/jupiter/api/RepeatedTest;",
          "Lorg/junit/jupiter/api/TestFactory;",
          "Lorg/junit/jupiter/api/TestTemplate;",
          "Lorg/junit/jupiter/params/ParameterizedTest;");

  @Override
  public void execute(Task task) {
    Test test = (Test) task;
    if (!(test.getOptions() instanceof TestNGOptions)) {
      return;
    }
    Set<String> offenders = new TreeSet<>();
    test.getTestClassesDirs()
        .getAsFileTree()
        .matching(p -> p.include("**/*.class"))
        .visit(
            details -> {
              if (details.isDirectory()) {
                return;
              }
              String text;
              try {
                // ISO-8859-1 maps bytes 1:1, so the ASCII descriptors match the constant pool.
                text =
                    new String(
                        Files.readAllBytes(details.getFile().toPath()), StandardCharsets.ISO_8859_1);
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
              if (JUNIT_TEST_ANNOTATIONS.stream().anyMatch(text::contains)) {
                offenders.add(
                    details
                        .getRelativePath()
                        .getPathString()
                        .replaceAll("(\\$.*)?\\.class$", "")
                        .replace('/', '.'));
              }
            });
    if (!offenders.isEmpty()) {
      throw new GradleException(
          test.getPath()
              + " runs TestNG, which silently skips JUnit tests, but these classes use JUnit test"
              + " annotations: "
              + String.join(", ", offenders)
              + ". Use org.testng.annotations.Test (and TestNG lifecycle/assert APIs) instead.");
    }
  }
}

package com.linkedin.gms;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;
import static org.testng.Assert.assertTrue;

import com.tngtech.archunit.core.domain.JavaAnnotation;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaParameter;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.Test;

/**
 * A Spring MVC mapping that declares {@code consumes} must read a request body.
 *
 * <p>{@code consumes} only narrows which requests match by their {@code Content-Type}. On a handler
 * that never reads a body it turns a client that sends no body (and so no {@code Content-Type})
 * into a 415, while MockMvc tests that always set the header stay green. Either take the body
 * ({@code @RequestBody}, {@code @RequestPart}, {@code HttpEntity}) or drop {@code consumes}. Form
 * and multipart mappings may read their body through {@code @RequestParam} or
 * {@code @ModelAttribute}.
 */
public class ConsumesRequiresBodyArchTest {

  private static final String REQUEST_MAPPING =
      "org.springframework.web.bind.annotation.RequestMapping";
  private static final Set<String> BODY_ANNOTATIONS =
      Set.of(
          "org.springframework.web.bind.annotation.RequestBody",
          "org.springframework.web.bind.annotation.RequestPart");
  private static final Set<String> FORM_ANNOTATIONS =
      Set.of(
          "org.springframework.web.bind.annotation.RequestParam",
          "org.springframework.web.bind.annotation.ModelAttribute");
  private static final String HTTP_ENTITY = "org.springframework.http.HttpEntity";

  /**
   * Existing mappings that declare {@code consumes} without reading a body, as {@code
   * Class#method}. May only shrink: fix a mapping, then delete its line.
   */
  private static final Set<String> BASELINE = Set.of();

  private static final JavaClasses CLASSES =
      new ClassFileImporter()
          .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
          .withImportOption(location -> location.contains("/build/"))
          .importPackages(
              "com.linkedin.metadata",
              "com.linkedin.datahub",
              "com.linkedin.gms",
              "com.datahub",
              "io.datahubproject");

  @Test
  public void mappingsThatDeclareConsumesTakeABody() {
    assertTrue(
        CLASSES.stream()
            .flatMap(c -> c.getMethods().stream())
            .anyMatch(m -> !consumes(m).isEmpty()),
        "no mapping with consumes found; is the openapi servlet still on the test classpath?");

    methods()
        .that()
        .areAnnotatedWith(REQUEST_MAPPING)
        .or()
        .areMetaAnnotatedWith(REQUEST_MAPPING)
        .should(readABodyIfTheyDeclareConsumes())
        .check(CLASSES);
  }

  @Test
  public void baselineOnlyListsCurrentViolations() {
    for (String entry : BASELINE) {
      String[] parts = entry.split("#");
      JavaClass owner = CLASSES.get(parts[0]);
      List<JavaMethod> matching =
          owner.getMethods().stream().filter(m -> m.getName().equals(parts[1])).toList();
      assertTrue(
          matching.stream().anyMatch(m -> !consumes(m).isEmpty() && !readsBody(m)),
          entry + " no longer violates the rule; remove it from BASELINE");
    }
  }

  private static ArchCondition<JavaMethod> readABodyIfTheyDeclareConsumes() {
    return new ArchCondition<>("read a request body if they declare consumes") {
      @Override
      public void check(JavaMethod method, ConditionEvents events) {
        List<String> consumes = consumes(method);
        if (consumes.isEmpty()
            || readsBody(method)
            || BASELINE.contains(method.getOwner().getName() + "#" + method.getName())) {
          return;
        }
        events.add(
            SimpleConditionEvent.violated(
                method,
                method.getFullName()
                    + " declares consumes="
                    + consumes
                    + " but takes no @RequestBody/@RequestPart; drop consumes or read the body"));
      }
    };
  }

  /**
   * The media types the mapping consumes: its own {@code consumes}, else the class-level {@code
   * consumes}, which Spring applies to every method of the controller.
   */
  private static List<String> consumes(JavaMethod method) {
    Optional<List<String>> own = mappingConsumes(method.getAnnotations());
    if (own.isPresent() && !own.get().isEmpty()) {
      return own.get();
    }
    if (own.isEmpty()) {
      return List.of();
    }
    return mappingConsumes(method.getOwner().getAnnotations()).orElse(List.of());
  }

  /** Empty if none of the annotations is a mapping; else that mapping's consumes. */
  private static Optional<List<String>> mappingConsumes(
      Set<? extends JavaAnnotation<?>> annotations) {
    return annotations.stream()
        .filter(
            a ->
                a.getRawType().getName().equals(REQUEST_MAPPING)
                    || a.getRawType().isAnnotatedWith(REQUEST_MAPPING))
        .findFirst()
        .map(a -> a.get("consumes").map(v -> Arrays.asList((String[]) v)).orElse(List.of()));
  }

  private static boolean readsBody(JavaMethod method) {
    boolean form =
        consumes(method).stream()
            .anyMatch(
                t ->
                    t.startsWith("multipart/")
                        || t.startsWith("application/x-www-form-urlencoded"));
    for (JavaParameter p : method.getParameters()) {
      if (p.getRawType().isAssignableTo(HTTP_ENTITY)) {
        return true;
      }
      for (JavaAnnotation<?> a : p.getAnnotations()) {
        String type = a.getRawType().getName();
        if (BODY_ANNOTATIONS.contains(type) || (form && FORM_ANNOTATIONS.contains(type))) {
          return true;
        }
      }
    }
    return false;
  }
}

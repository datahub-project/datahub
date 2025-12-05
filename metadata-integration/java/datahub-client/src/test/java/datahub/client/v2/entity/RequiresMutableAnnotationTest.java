package datahub.client.v2.entity;

import static org.junit.Assert.fail;

import datahub.client.v2.annotations.RequiresMutable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * Test to enforce that all mutation methods on Entity classes have the @RequiresMutable annotation.
 *
 * <p>This test ensures developers don't forget to annotate mutation methods, which would bypass
 * read-only entity protection.
 */
public class RequiresMutableAnnotationTest {

  /**
   * List of all Entity subclasses to validate.
   *
   * <p>Update this list when new entity types are added.
   */
  private static final Class<?>[] ENTITY_CLASSES = {
    Chart.class,
    Dataset.class,
    Dashboard.class,
    DataFlow.class,
    DataJob.class,
    Container.class,
    MLModel.class,
    MLModelGroup.class
  };

  /**
   * Verifies that all mutation methods have @RequiresMutable annotation.
   *
   * <p>Mutation methods are identified by naming convention: - set* (setters) - add* (adders) -
   * remove* (removers)
   *
   * <p>Exemptions: - Static methods - Methods annotated with @SkipMutabilityCheck (for future use)
   * - Protected/private methods (internal helpers)
   */
  @Test
  public void testAllMutationMethodsHaveRequiresMutableAnnotation() {
    List<String> violations = new ArrayList<>();

    for (Class<?> entityClass : ENTITY_CLASSES) {
      for (Method method : entityClass.getDeclaredMethods()) {
        if (isMutationMethod(method) && !hasRequiresMutableAnnotation(method)) {
          violations.add(
              String.format(
                  "%s.%s() is a mutation method but lacks @RequiresMutable annotation",
                  entityClass.getSimpleName(), method.getName()));
        }
      }
    }

    if (!violations.isEmpty()) {
      fail(
          "Found mutation methods without @RequiresMutable annotation:\n  - "
              + String.join("\n  - ", violations));
    }
  }

  /**
   * Determines if a method is a mutation method based on naming convention.
   *
   * @param method the method to check
   * @return true if this is a mutation method that should be annotated
   */
  private boolean isMutationMethod(Method method) {
    String name = method.getName();
    int modifiers = method.getModifiers();

    // Must be public (non-static, non-abstract)
    if (!Modifier.isPublic(modifiers)
        || Modifier.isStatic(modifiers)
        || Modifier.isAbstract(modifiers)) {
      return false;
    }

    // Must match mutation naming pattern
    if (!name.matches("(set|add|remove).*")) {
      return false;
    }

    // Exclude builder-related methods
    if (name.equals("setMode") || name.equals("setOperationMode")) {
      return false; // Internal mode setters
    }

    return true;
  }

  /**
   * Checks if a method has the @RequiresMutable annotation.
   *
   * @param method the method to check
   * @return true if annotated with @RequiresMutable
   */
  private boolean hasRequiresMutableAnnotation(Method method) {
    return method.isAnnotationPresent(RequiresMutable.class);
  }
}

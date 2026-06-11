package com.linkedin.metadata.entity;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Shared ArchUnit helpers for enforcing parameter-shape rules on a target class. Anyone can call
 * {@link #checkArch(Class, Map)} to assert that every public, non-exempt method declared directly
 * on a target type has the required parameter type at a specific index — e.g. {@code Map.of(0,
 * OperationContext.class)} to require {@code OperationContext} as the first parameter.
 *
 * <p>Currently filters to {@code arePublic() && !@OperationContextExempt}. A future overload can
 * accept a visibility filter map (private / protected / public) and a custom exempt annotation if
 * more arch rules want to plug in. Add the overload when the first caller needs it; until then keep
 * the surface small.
 */
public final class OperationContextArchTestUtil {

  private OperationContextArchTestUtil() {}

  /**
   * Asserts that every public, non-exempt method declared directly on {@code targetClass} has the
   * types specified by {@code requiredAtIndex} at the corresponding parameter positions. Methods
   * carrying {@link OperationContextExempt} are skipped.
   *
   * @param targetClass the class/interface whose methods are checked
   * @param requiredAtIndex map of {@code parameterIndex → required type}; all entries must be
   *     satisfied for a method to pass
   */
  public static void checkArch(Class<?> targetClass, Map<Integer, Class<?>> requiredAtIndex) {
    // Imports only the single class file. Sufficient for parameter-type checks on methods declared
    // directly in targetClass. If future rules need cross-class analysis (e.g. checking
    // implementations), switch to importPackagesOf(targetClass).
    var classes = new ClassFileImporter().importClasses(targetClass);
    methods()
        .that()
        .areDeclaredIn(targetClass)
        .and()
        .arePublic()
        .and()
        .areNotAnnotatedWith(OperationContextExempt.class)
        .should(haveRequiredParamTypes(requiredAtIndex))
        .check(classes);
  }

  private static ArchCondition<JavaMethod> haveRequiredParamTypes(
      Map<Integer, Class<?>> requiredAtIndex) {
    String description =
        "have required parameter types — "
            + requiredAtIndex.entrySet().stream()
                .map(e -> "param[" + e.getKey() + "]=" + e.getValue().getSimpleName())
                .collect(Collectors.joining(", "));
    return new ArchCondition<>(description) {
      @Override
      public void check(JavaMethod method, ConditionEvents events) {
        List<JavaClass> params = method.getRawParameterTypes();
        for (Map.Entry<Integer, Class<?>> entry : requiredAtIndex.entrySet()) {
          int idx = entry.getKey();
          Class<?> expectedType = entry.getValue();
          boolean satisfied = idx < params.size() && params.get(idx).isAssignableTo(expectedType);
          if (!satisfied) {
            String actual =
                idx < params.size() ? params.get(idx).getSimpleName() : "<no parameter>";
            String expected = expectedType.getSimpleName();
            events.add(
                SimpleConditionEvent.violated(
                    method,
                    String.format(
                        "Method %s.%s: parameter[%d] is <%s>, expected <%s>. "
                            + "Add %s at index %d, or annotate with "
                            + "@OperationContextExempt(reason=\"...\") if legitimately exempt.",
                        method.getOwner().getSimpleName(),
                        method.getName(),
                        idx,
                        actual,
                        expected,
                        expected,
                        idx)));
          }
        }
      }
    };
  }
}

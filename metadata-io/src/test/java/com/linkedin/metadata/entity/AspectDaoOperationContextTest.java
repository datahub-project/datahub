package com.linkedin.metadata.entity;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Architectural rule: every public method declared directly on {@link AspectDao} must have the
 * required parameter types at the specified indices, unless annotated with {@link
 * OperationContextExempt}.
 *
 * <p>Use {@link #checkArch(Class, Map)} to add enforcement for additional DAO interfaces. The
 * {@code requiredAtIndex} map encodes which type is required at which parameter position — e.g.
 * {@code Map.of(0, OperationContext.class)} means the first parameter must be (or extend) {@code
 * OperationContext}.
 *
 * <p>This test is expected to <strong>fail</strong> until the OperationContext threading refactor
 * (opcontext-pr1-dao-signatures) is merged.
 */
public class AspectDaoOperationContextTest {

  @Test
  public void aspectDaoPublicMethodsMustHaveOperationContextAsFirstParam() {
    checkArch(AspectDao.class, Map.of(0, OperationContext.class));
  }

  // ---------------------------------------------------------------------------
  // Reusable helper — add one @Test per interface you want to enforce
  // ---------------------------------------------------------------------------

  /**
   * Asserts that every public, non-exempt method declared directly on {@code daoInterface} has the
   * types specified by {@code requiredAtIndex} at the corresponding parameter positions.
   *
   * @param daoInterface the interface whose methods are checked
   * @param requiredAtIndex map of {@code parameterIndex → required type}; all entries must be
   *     satisfied for a method to pass
   */
  private static void checkArch(Class<?> daoInterface, Map<Integer, Class<?>> requiredAtIndex) {
    var classes = new ClassFileImporter().importClasses(daoInterface);
    methods()
        .that()
        .areDeclaredIn(daoInterface)
        .and()
        .arePublic()
        .and()
        .areNotAnnotatedWith(OperationContextExempt.class)
        .should(haveRequiredParamTypes(requiredAtIndex))
        .check(classes);
  }

  private static ArchCondition<JavaMethod> haveRequiredParamTypes(
      Map<Integer, Class<?>> requiredAtIndex) {
    String description = "have required parameter types at indices " + requiredAtIndex.keySet();
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
                        expectedType.getSimpleName(),
                        expectedType.getSimpleName(),
                        idx)));
          }
        }
      }
    };
  }
}

package datahub.client.v2.entity;

import static org.junit.Assert.fail;

import datahub.client.v2.annotations.RequiresMutable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Test to enforce that all @RequiresMutable methods actually call checkNotReadOnly().
 *
 * <p>This test uses bytecode inspection to verify the implementation matches the annotation.
 * Developers might annotate a method but forget to add the actual check call.
 */
public class RequiresMutableImplementationTest {

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
   * Verifies that all @RequiresMutable methods call checkNotReadOnly().
   *
   * <p>Uses ASM bytecode inspection to verify the method body contains an INVOKEVIRTUAL instruction
   * calling checkNotReadOnly.
   */
  @Test
  public void testRequiresMutableMethodsCallCheckNotReadOnly() throws IOException {
    List<String> violations = new ArrayList<>();

    for (Class<?> entityClass : ENTITY_CLASSES) {
      for (Method method : entityClass.getDeclaredMethods()) {
        if (method.isAnnotationPresent(RequiresMutable.class)) {
          if (!methodCallsCheckNotReadOnly(entityClass, method)) {
            violations.add(
                String.format(
                    "%s.%s() has @RequiresMutable but doesn't call checkNotReadOnly()",
                    entityClass.getSimpleName(), method.getName()));
          }
        }
      }
    }

    if (!violations.isEmpty()) {
      fail(
          "Found @RequiresMutable methods that don't call checkNotReadOnly():\n  - "
              + String.join("\n  - ", violations));
    }
  }

  /**
   * Inspects bytecode to check if a method calls checkNotReadOnly().
   *
   * @param clazz the class containing the method
   * @param method the method to inspect
   * @return true if the method calls checkNotReadOnly()
   */
  private boolean methodCallsCheckNotReadOnly(Class<?> clazz, Method method) throws IOException {
    String className = clazz.getName().replace('.', '/');
    String methodName = method.getName();
    String methodDescriptor = getMethodDescriptor(method);

    // Load class bytecode
    InputStream classStream = clazz.getClassLoader().getResourceAsStream(className + ".class");
    if (classStream == null) {
      throw new IOException("Could not load class file for " + clazz.getName());
    }

    ClassReader reader = new ClassReader(classStream);
    MethodCallDetector detector = new MethodCallDetector(methodName, methodDescriptor);
    reader.accept(detector, 0);

    return detector.foundCheckNotReadOnly;
  }

  /**
   * Generates method descriptor from Method object.
   *
   * @param method the method
   * @return method descriptor (e.g., "(Ljava/lang/String;)Ldatahub/client/v2/entity/Chart;")
   */
  private String getMethodDescriptor(Method method) {
    StringBuilder descriptor = new StringBuilder("(");

    // Parameter types
    for (Class<?> paramType : method.getParameterTypes()) {
      descriptor.append(getTypeDescriptor(paramType));
    }

    descriptor.append(")");

    // Return type
    descriptor.append(getTypeDescriptor(method.getReturnType()));

    return descriptor.toString();
  }

  /**
   * Converts a Java type to JVM type descriptor.
   *
   * @param type the Java type
   * @return JVM type descriptor
   */
  private String getTypeDescriptor(Class<?> type) {
    if (type == void.class) return "V";
    if (type == boolean.class) return "Z";
    if (type == byte.class) return "B";
    if (type == char.class) return "C";
    if (type == short.class) return "S";
    if (type == int.class) return "I";
    if (type == long.class) return "J";
    if (type == float.class) return "F";
    if (type == double.class) return "D";
    if (type.isArray()) return "[" + getTypeDescriptor(type.getComponentType());
    return "L" + type.getName().replace('.', '/') + ";";
  }

  /** ASM ClassVisitor to detect method calls. */
  private static class MethodCallDetector extends ClassVisitor {
    private final String targetMethodName;
    private final String targetMethodDescriptor;
    boolean foundCheckNotReadOnly = false;

    MethodCallDetector(String methodName, String methodDescriptor) {
      super(Opcodes.ASM9);
      this.targetMethodName = methodName;
      this.targetMethodDescriptor = methodDescriptor;
    }

    @Override
    public MethodVisitor visitMethod(
        int access, String name, String descriptor, String signature, String[] exceptions) {
      // Only inspect the target method
      if (name.equals(targetMethodName) && descriptor.equals(targetMethodDescriptor)) {
        return new MethodVisitor(Opcodes.ASM9) {
          @Override
          public void visitMethodInsn(
              int opcode, String owner, String name, String descriptor, boolean isInterface) {
            // Check for INVOKEVIRTUAL or INVOKESPECIAL on checkNotReadOnly
            if ((opcode == Opcodes.INVOKEVIRTUAL || opcode == Opcodes.INVOKESPECIAL)
                && name.equals("checkNotReadOnly")
                && descriptor.equals("(Ljava/lang/String;)V")) {
              foundCheckNotReadOnly = true;
            }
            super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
          }
        };
      }
      return null;
    }
  }
}

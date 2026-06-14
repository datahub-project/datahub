package com.linkedin.datahub.graphql.resolvers.entity;

import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.graphql.generated.EntityPrivileges;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

/**
 * Two-way drift guard for {@link EntityPrivilegesFields} against the generated {@link
 * EntityPrivileges} type.
 *
 * <p>{@code EntityPrivilegesResolver} keys the GraphQL selection set off these constants, so a typo
 * or drift (a field added/removed in the GraphQL definition, which regenerates {@code
 * EntityPrivileges}) would silently leave a privilege uncomputed. The two tests below check each
 * direction independently so a failure points straight at the cause.
 */
public class EntityPrivilegesFieldsTest {

  /** Direction 1: every declared constant must map to a field on the generated type. */
  @Test
  public void testEveryConstantMapsToAGeneratedField() {
    Set<String> constantsWithoutField = new HashSet<>(declaredConstants());
    constantsWithoutField.removeAll(generatedFields());
    assertTrue(
        constantsWithoutField.isEmpty(),
        "EntityPrivilegesFields constant(s) do not match any field on the generated "
            + "EntityPrivileges type (typo or stale constant): "
            + constantsWithoutField);
  }

  /** Direction 2: every field on the generated type must have a declared constant. */
  @Test
  public void testEveryGeneratedFieldHasAConstant() {
    Set<String> fieldsWithoutConstant = new HashSet<>(generatedFields());
    fieldsWithoutConstant.removeAll(declaredConstants());
    assertTrue(
        fieldsWithoutConstant.isEmpty(),
        "Generated EntityPrivileges field(s) have no EntityPrivilegesFields constant (drift — add "
            + "a constant and wire it in EntityPrivilegesResolver): "
            + fieldsWithoutConstant);
  }

  private static Set<String> generatedFields() {
    return Arrays.stream(EntityPrivileges.class.getDeclaredFields())
        .filter(f -> !f.isSynthetic() && !Modifier.isStatic(f.getModifiers()))
        .map(Field::getName)
        .collect(Collectors.toSet());
  }

  private static Set<String> declaredConstants() {
    return Arrays.stream(EntityPrivilegesFields.class.getDeclaredFields())
        .filter(f -> Modifier.isStatic(f.getModifiers()) && f.getType() == String.class)
        .map(EntityPrivilegesFieldsTest::constantValue)
        .collect(Collectors.toSet());
  }

  private static String constantValue(Field field) {
    try {
      return (String) field.get(null);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Unable to read constant " + field.getName(), e);
    }
  }
}

package com.linkedin.datahub.graphql.resolvers.logical;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.datahub.graphql.generated.CreateLogicalModelInput;
import com.linkedin.datahub.graphql.generated.FabricType;
import org.testng.annotations.Test;

/** N10: createLogicalModel takes a custom env in envName; env keeps its old meaning. */
public class CreateLogicalModelEnvTest {

  @Test
  public void testEnvBehavesAsBefore() {
    assertEquals(CreateLogicalModelResolver.resolveEnv(input(null, null)), "PROD");
    assertEquals(CreateLogicalModelResolver.resolveEnv(input(FabricType.DEV, null)), "DEV");
  }

  @Test
  public void testEnvNameTakesPrecedence() {
    assertEquals(CreateLogicalModelResolver.resolveEnv(input(null, "MY_ENV")), "MY_ENV");
    assertEquals(CreateLogicalModelResolver.resolveEnv(input(FabricType.DEV, "MY_ENV")), "MY_ENV");
  }

  @Test
  public void testOtherIsNotAnEnvironment() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CreateLogicalModelResolver.resolveEnv(input(FabricType.OTHER, null)));
  }

  private static CreateLogicalModelInput input(FabricType env, String envName) {
    CreateLogicalModelInput input = new CreateLogicalModelInput();
    input.setName("model");
    input.setEnv(env);
    input.setEnvName(envName);
    return input;
  }
}

package com.linkedin.datahub.graphql.authorization;

import static org.testng.Assert.assertEquals;

import com.linkedin.datahub.graphql.generated.ViewProperties;
import org.testng.annotations.Test;

public class AuthorizationUtilsTest {

  @Test
  public void testRestrictedViewProperties() {
    // provides a test of primitive boolean
    ViewProperties viewProperties =
        ViewProperties.builder()
            .setMaterialized(true)
            .setLanguage("testLang")
            .setFormattedLogic("formattedLogic")
            .setLogic("testLogic")
            .build();

    String expected =
        ViewProperties.builder()
            .setMaterialized(true)
            .setLanguage("")
            .setLogic("")
            .build()
            .toString();

    assertEquals(
        AuthorizationUtils.restrictEntity(viewProperties, ViewProperties.class).toString(),
        expected);
  }
}

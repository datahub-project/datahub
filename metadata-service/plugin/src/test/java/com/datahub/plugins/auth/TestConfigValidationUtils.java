package com.datahub.plugins.auth;

import com.datahub.plugins.common.ConfigValidationUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

@Test
public class TestConfigValidationUtils {

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testWhiteSpacesValidation() {
    ConfigValidationUtils.whiteSpacesValidation("name", "plugin name with spaces");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testMapShouldNotBeEmpty() {
    ConfigValidationUtils.mapShouldNotBeEmpty("configs", Collections.emptyMap());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testListShouldNotBeEmpty() {
    ConfigValidationUtils.listShouldNotBeEmpty("plugins", Collections.emptyList());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testListShouldNotHaveDuplicate() {
    List<String> list = new ArrayList<>();
    list.add("ranger-authorizer");
    list.add("ranger-authorizer");
    ConfigValidationUtils.listShouldNotHaveDuplicate("plugins", list);
  }
}

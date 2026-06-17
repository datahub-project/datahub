package com.linkedin.metadata.models.annotation;

import static org.testng.Assert.*;

import com.linkedin.data.DataMap;
import com.linkedin.metadata.models.ModelValidationException;
import org.testng.annotations.Test;

public class SystemDataVisibilityTest {

  @Test
  public void testBooleanShorthandFullyHidden() {
    SystemDataVisibility visibility =
        SystemDataVisibility.fromSchemaProperty(true, "SystemEntity", "test.Context");
    assertTrue(visibility.isPresent());
    assertFalse(visibility.isAllowRead());
    assertFalse(visibility.isAllowExists());
  }

  @Test
  public void testMapFormOptInFlags() {
    DataMap map = new DataMap();
    map.put("allowRead", true);
    map.put("allowExists", true);
    SystemDataVisibility visibility =
        SystemDataVisibility.fromSchemaProperty(map, "SystemEntity", "test.Context");
    assertTrue(visibility.isAllowRead());
    assertTrue(visibility.isAllowExists());
  }

  @Test(expectedExceptions = ModelValidationException.class)
  public void testBooleanFalseRejected() {
    SystemDataVisibility.fromSchemaProperty(false, "System", "test.Context");
  }
}

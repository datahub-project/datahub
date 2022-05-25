package com.datahub.authorizer.plugin.ranger;

import java.util.List;
import java.util.Map;
import org.junit.Test;


public class TestDatahubRangerAuthPlugin {
  @Test
  public void testValidateConfig() throws Exception {
    DatahubRangerAuthPlugin datahubRangerAuthPlugin = new DatahubRangerAuthPlugin();
    Map<String, Object> map = datahubRangerAuthPlugin.validateConfig();
    assert map != null;
  }

  @Test
  public void testLookupResource() throws Exception {
    DatahubRangerAuthPlugin datahubRangerAuthPlugin = new DatahubRangerAuthPlugin();
    List<String> resources = datahubRangerAuthPlugin.lookupResource(null);
    assert resources != null;
  }
}

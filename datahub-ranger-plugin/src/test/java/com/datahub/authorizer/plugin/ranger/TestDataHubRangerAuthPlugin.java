package com.datahub.authorizer.plugin.ranger;

import java.util.List;
import java.util.Map;
import org.junit.Test;


public class TestDataHubRangerAuthPlugin {
  @Test
  public void testValidateConfig() throws Exception {
    DataHubRangerAuthPlugin datahubRangerAuthPlugin = new DataHubRangerAuthPlugin();
    Map<String, Object> map = datahubRangerAuthPlugin.validateConfig();
    assert map != null;
  }

  @Test
  public void testLookupResource() throws Exception {
    DataHubRangerAuthPlugin datahubRangerAuthPlugin = new DataHubRangerAuthPlugin();
    List<String> resources = datahubRangerAuthPlugin.lookupResource(null);
    assert resources != null;
  }
}

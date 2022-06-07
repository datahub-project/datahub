package com.datahub.authorizer.plugin.ranger;

import org.junit.Test;


public class TestDataHubRangerAuthPlugin {
  @Test(expected = UnsupportedOperationException.class)
  public void testValidateConfig() throws Exception {
    DataHubRangerAuthPlugin datahubRangerAuthPlugin = new DataHubRangerAuthPlugin();
    datahubRangerAuthPlugin.validateConfig();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testLookupResource() throws Exception {
    DataHubRangerAuthPlugin datahubRangerAuthPlugin = new DataHubRangerAuthPlugin();
    datahubRangerAuthPlugin.lookupResource(null);
  }
}

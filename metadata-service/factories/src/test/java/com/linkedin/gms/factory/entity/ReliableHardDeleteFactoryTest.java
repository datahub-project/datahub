package com.linkedin.gms.factory.entity;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.async.delete.ReliableHardDelete;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ReliableHardDeleteFactoryTest {

  @DataProvider(name = "flag")
  public Object[][] flag() {
    return new Object[][] {{true}, {false}};
  }

  /** The bean carries the kill switch every entry point reads. */
  @Test(dataProvider = "flag")
  public void theReliableHardDeleteFollowsTheFlag(final boolean enabled) {
    final FeatureFlags featureFlags = new FeatureFlags();
    featureFlags.setReliableHardDelete(enabled);
    final ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.getFeatureFlags()).thenReturn(featureFlags);

    try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
      context.registerBean("entityService", EntityService.class, () -> mock(EntityService.class));
      context.registerBean(
          "configurationProvider", ConfigurationProvider.class, () -> configurationProvider);
      context.register(ReliableHardDeleteFactory.class);
      context.refresh();

      assertEquals(context.getBean(ReliableHardDelete.class).isEnabled(), enabled);
    }
  }
}

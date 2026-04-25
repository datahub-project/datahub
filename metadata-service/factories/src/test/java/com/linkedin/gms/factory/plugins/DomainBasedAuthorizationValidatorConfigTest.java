package com.linkedin.gms.factory.plugins;

import static org.testng.Assert.*;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.validation.DomainBasedAuthorizationValidator;
import com.linkedin.metadata.config.DataHubConfiguration;
import java.util.List;
import org.mockito.Answers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test for DomainBasedAuthorizationValidator bean creation when domain-based authorization is
 * enabled. This covers SpringStandardPluginConfiguration lines 378-392 which create the
 * domainBasedAuthorizationValidator bean.
 */
@SpringBootTest(classes = SpringStandardPluginConfiguration.class)
@TestPropertySource(
    properties = {
      // Enable domain-based authorization to trigger bean creation
      "authorization.defaultAuthorizer.domainBasedAuthorizationEnabled=true",
      // Required for other beans
      "metadataChangeProposal.validation.ignoreUnknown=true",
      "metadataChangeProposal.validation.extensions.enabled=false"
    })
public class DomainBasedAuthorizationValidatorConfigTest extends AbstractTestNGSpringContextTests {

  @Autowired private ApplicationContext context;

  @MockitoBean(answers = Answers.RETURNS_MOCKS)
  private ConfigurationProvider configurationProvider;

  @BeforeClass
  private void setup() {
    Mockito.when(configurationProvider.getDatahub()).thenReturn(new DataHubConfiguration());
  }

  @Test
  public void testDomainBasedAuthorizationValidatorBeanCreation() {
    assertTrue(context.containsBean("domainBasedAuthorizationValidator"));
    AspectPayloadValidator validator =
        context.getBean("domainBasedAuthorizationValidator", AspectPayloadValidator.class);
    assertNotNull(validator);
    assertTrue(validator instanceof DomainBasedAuthorizationValidator);

    // Verify configuration
    assertNotNull(validator.getConfig());
    assertTrue(validator.getConfig().isEnabled());
    assertEquals(
        validator.getConfig().getClassName(), DomainBasedAuthorizationValidator.class.getName());

    // Verify supported operations match DOMAIN_AUTH_CHANGE_TYPE_OPERATIONS
    assertEquals(
        validator.getConfig().getSupportedOperations(),
        List.of("CREATE", "CREATE_ENTITY", "UPDATE", "UPSERT", "PATCH", "RESTATE"));
  }
}

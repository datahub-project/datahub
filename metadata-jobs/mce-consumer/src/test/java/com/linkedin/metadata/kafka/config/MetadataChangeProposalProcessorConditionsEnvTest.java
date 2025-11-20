package com.linkedin.metadata.kafka.config;

import static org.mockito.Mockito.mock;

import com.linkedin.metadata.kafka.config.batch.BatchMetadataChangeProposalProcessorCondition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

@SpringBootTest(
    classes = {
      MetadataChangeProposalProcessorCondition.class,
      BatchMetadataChangeProposalProcessorCondition.class
    })
@TestPropertySource(properties = {"MCP_CONSUMER_BATCH_ENABLED=true", "MCP_CONSUMER_ENABLED=true"})
public class MetadataChangeProposalProcessorConditionsEnvTest
    extends AbstractTestNGSpringContextTests {

  private AnnotatedTypeMetadata mockMetadata = mock(AnnotatedTypeMetadata.class);

  @Autowired private Environment environment;

  @Autowired private MetadataChangeProposalProcessorCondition regularCondition;

  @Autowired private BatchMetadataChangeProposalProcessorCondition batchCondition;

  private ConditionContext createContextWithCurrentEnvironment() {
    return new ConditionContext() {
      @Override
      public Environment getEnvironment() {
        return environment;
      }

      // Implement other methods with null returns
      @Override
      public BeanDefinitionRegistry getRegistry() {
        return null;
      }

      @Override
      public ConfigurableListableBeanFactory getBeanFactory() {
        return null;
      }

      @Override
      public ResourceLoader getResourceLoader() {
        return null;
      }

      @Override
      public ClassLoader getClassLoader() {
        return null;
      }
    };
  }

  @Test
  public void testConditions() {
    Assert.assertFalse(
        regularCondition.matches(createContextWithCurrentEnvironment(), mockMetadata));
    Assert.assertTrue(batchCondition.matches(createContextWithCurrentEnvironment(), mockMetadata));
  }
}

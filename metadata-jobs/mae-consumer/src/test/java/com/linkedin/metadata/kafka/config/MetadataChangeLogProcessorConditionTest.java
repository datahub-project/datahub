package com.linkedin.metadata.kafka.config;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MetadataChangeLogProcessorConditionTest {

  @Mock private ConditionContext mockConditionContext;
  @Mock private AnnotatedTypeMetadata mockAnnotatedTypeMetadata;
  @Mock private Environment mockEnvironment;

  private MetadataChangeLogProcessorCondition condition;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    condition = new MetadataChangeLogProcessorCondition();
    when(mockConditionContext.getEnvironment()).thenReturn(mockEnvironment);
  }

  @Test
  public void testMatchesWithMCLProcessingDisabled() {
    // MCL processing is disabled
    when(mockEnvironment.getProperty("MAE_CONSUMER_ENABLED")).thenReturn("false");
    when(mockEnvironment.getProperty("MCL_CONSUMER_ENABLED")).thenReturn("false");

    boolean result = condition.matches(mockConditionContext, mockAnnotatedTypeMetadata);

    assertFalse(result);
  }

  @Test
  public void testMatchesWithMAEConsumerEnabled() {
    // MAE consumer is enabled
    when(mockEnvironment.getProperty("MAE_CONSUMER_ENABLED")).thenReturn("true");
    when(mockEnvironment.getProperty("MCL_CONSUMER_ENABLED")).thenReturn("false");

    boolean result = condition.matches(mockConditionContext, mockAnnotatedTypeMetadata);

    assertTrue(result);
  }

  @Test
  public void testMatchesWithMCLConsumerEnabled() {
    // MCL consumer is enabled
    when(mockEnvironment.getProperty("MAE_CONSUMER_ENABLED")).thenReturn("false");
    when(mockEnvironment.getProperty("MCL_CONSUMER_ENABLED")).thenReturn("true");

    boolean result = condition.matches(mockConditionContext, mockAnnotatedTypeMetadata);

    assertTrue(result);
  }

  @Test
  public void testMatchesWithBothConsumersEnabled() {
    // Both consumers are enabled
    when(mockEnvironment.getProperty("MAE_CONSUMER_ENABLED")).thenReturn("true");
    when(mockEnvironment.getProperty("MCL_CONSUMER_ENABLED")).thenReturn("true");

    boolean result = condition.matches(mockConditionContext, mockAnnotatedTypeMetadata);

    assertTrue(result);
  }

  @Test
  public void testMatchesWithNullEnvironmentProperties() {
    // Environment properties are null
    when(mockEnvironment.getProperty("MAE_CONSUMER_ENABLED")).thenReturn(null);
    when(mockEnvironment.getProperty("MCL_CONSUMER_ENABLED")).thenReturn(null);

    boolean result = condition.matches(mockConditionContext, mockAnnotatedTypeMetadata);

    assertFalse(result);
  }

  @Test
  public void testMatchesWithEmptyEnvironmentProperties() {
    // Environment properties are empty strings
    when(mockEnvironment.getProperty("MAE_CONSUMER_ENABLED")).thenReturn("");
    when(mockEnvironment.getProperty("MCL_CONSUMER_ENABLED")).thenReturn("");

    boolean result = condition.matches(mockConditionContext, mockAnnotatedTypeMetadata);

    assertFalse(result);
  }

  @Test
  public void testMatchesWithInvalidEnvironmentProperties() {
    // Environment properties are not "true"
    when(mockEnvironment.getProperty("MAE_CONSUMER_ENABLED")).thenReturn("invalid");
    when(mockEnvironment.getProperty("MCL_CONSUMER_ENABLED")).thenReturn("invalid");

    boolean result = condition.matches(mockConditionContext, mockAnnotatedTypeMetadata);

    assertFalse(result);
  }
}

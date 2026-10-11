package com.linkedin.datahub.upgrade.cleanup;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CleanupConditionTest {

  @Mock private ConditionContext mockConditionContext;
  @Mock private AnnotatedTypeMetadata mockAnnotatedTypeMetadata;
  @Mock private ConfigurableListableBeanFactory mockBeanFactory;
  @Mock private ApplicationArguments mockApplicationArguments;

  private CleanupCondition cleanupCondition;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    cleanupCondition = new CleanupCondition();
    when(mockConditionContext.getBeanFactory()).thenReturn(mockBeanFactory);
    when(mockBeanFactory.getBean(ApplicationArguments.class)).thenReturn(mockApplicationArguments);
  }

  @Test
  public void testMatchesWithCleanupArg() {
    List<String> nonOptionArgs = Arrays.asList("Cleanup");
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    assertTrue(cleanupCondition.matches(mockConditionContext, mockAnnotatedTypeMetadata));
  }

  @Test
  public void testDoesNotMatchWithOtherArgs() {
    List<String> nonOptionArgs = Arrays.asList("LoadIndices", "SystemUpdate");
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    assertFalse(cleanupCondition.matches(mockConditionContext, mockAnnotatedTypeMetadata));
  }

  @Test
  public void testMatchesWithMixedArgs() {
    List<String> nonOptionArgs = Arrays.asList("LoadIndices", "Cleanup", "SystemUpdate");
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    assertTrue(cleanupCondition.matches(mockConditionContext, mockAnnotatedTypeMetadata));
  }

  @Test
  public void testDoesNotMatchWithEmptyArgs() {
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(Collections.emptyList());

    assertFalse(cleanupCondition.matches(mockConditionContext, mockAnnotatedTypeMetadata));
  }

  @Test
  public void testDoesNotMatchWithNullArgs() {
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(null);

    assertFalse(cleanupCondition.matches(mockConditionContext, mockAnnotatedTypeMetadata));
  }

  @Test
  public void testMatchesWithNullElementsInArgs() {
    List<String> nonOptionArgs = Arrays.asList("LoadIndices", null, "Cleanup");
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    assertTrue(cleanupCondition.matches(mockConditionContext, mockAnnotatedTypeMetadata));
  }

  @Test
  public void testConstants() {
    assertEquals(CleanupCondition.CLEANUP_ARG, "Cleanup");
    assertTrue(CleanupCondition.CLEANUP_ARGS.contains("Cleanup"));
    assertEquals(CleanupCondition.CLEANUP_ARGS.size(), 1);
  }
}

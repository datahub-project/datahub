package com.linkedin.datahub.upgrade.conditions;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GeneralUpgradeConditionTest {

  @Mock private ConditionContext mockConditionContext;
  @Mock private AnnotatedTypeMetadata mockAnnotatedTypeMetadata;
  @Mock private ConfigurableListableBeanFactory mockBeanFactory;
  @Mock private ApplicationArguments mockApplicationArguments;

  private GeneralUpgradeCondition condition;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    condition = new GeneralUpgradeCondition();
    when(mockConditionContext.getBeanFactory()).thenReturn(mockBeanFactory);
    when(mockBeanFactory.getBean(ApplicationArguments.class)).thenReturn(mockApplicationArguments);
  }

  @Test
  public void testMatchesWhenNoExcludedArgs() {
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(Arrays.asList("SystemUpdate"));
    assertTrue(condition.matches(mockConditionContext, mockAnnotatedTypeMetadata));
  }

  @Test
  public void testMatchesWhenArgsEmpty() {
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(Collections.emptyList());
    assertTrue(condition.matches(mockConditionContext, mockAnnotatedTypeMetadata));
  }

  @Test
  public void testDoesNotMatchWhenLoadIndicesPresent() {
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(Arrays.asList("LoadIndices"));
    assertFalse(condition.matches(mockConditionContext, mockAnnotatedTypeMetadata));
  }

  @Test
  public void testDoesNotMatchWhenSqlSetupPresent() {
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(Arrays.asList("SqlSetup"));
    assertFalse(condition.matches(mockConditionContext, mockAnnotatedTypeMetadata));
  }

  @Test
  public void testDoesNotMatchWhenCleanupPresent() {
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(Arrays.asList("Cleanup"));
    assertFalse(condition.matches(mockConditionContext, mockAnnotatedTypeMetadata));
  }

  @Test
  public void testDoesNotMatchWhenExcludedArgMixedWithOthers() {
    when(mockApplicationArguments.getNonOptionArgs())
        .thenReturn(Arrays.asList("SystemUpdate", "LoadIndices"));
    assertFalse(condition.matches(mockConditionContext, mockAnnotatedTypeMetadata));
  }

  @Test
  public void testFilterOutNullArgs() {
    // null entries in the list should be filtered and not cause NPE
    when(mockApplicationArguments.getNonOptionArgs())
        .thenReturn(Arrays.asList(null, "SystemUpdate"));
    assertTrue(condition.matches(mockConditionContext, mockAnnotatedTypeMetadata));
  }

  @Test
  public void testMatchesWhenNonOptionArgsIsNull() {
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(null);
    assertTrue(condition.matches(mockConditionContext, mockAnnotatedTypeMetadata));
  }
}

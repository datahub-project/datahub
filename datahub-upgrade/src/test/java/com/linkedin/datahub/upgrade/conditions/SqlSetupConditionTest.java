package com.linkedin.datahub.upgrade.conditions;

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

public class SqlSetupConditionTest {

  @Mock private ConditionContext mockConditionContext;
  @Mock private AnnotatedTypeMetadata mockAnnotatedTypeMetadata;
  @Mock private ConfigurableListableBeanFactory mockBeanFactory;
  @Mock private ApplicationArguments mockApplicationArguments;

  private SqlSetupCondition sqlSetupCondition;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    sqlSetupCondition = new SqlSetupCondition();
    when(mockConditionContext.getBeanFactory()).thenReturn(mockBeanFactory);
    when(mockBeanFactory.getBean(ApplicationArguments.class)).thenReturn(mockApplicationArguments);
  }

  @Test
  public void testMatchesWithSqlSetupArg() {
    List<String> nonOptionArgs = Arrays.asList("SqlSetup");
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    boolean result = sqlSetupCondition.matches(mockConditionContext, mockAnnotatedTypeMetadata);

    assertTrue(result);
  }

  @Test
  public void testMatchesWithOtherArgs() {
    List<String> nonOptionArgs = Arrays.asList("LoadIndices", "SystemUpdate");
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    boolean result = sqlSetupCondition.matches(mockConditionContext, mockAnnotatedTypeMetadata);

    assertFalse(result);
  }

  @Test
  public void testMatchesWithMixedArgs() {
    List<String> nonOptionArgs = Arrays.asList("LoadIndices", "SqlSetup", "SystemUpdate");
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    boolean result = sqlSetupCondition.matches(mockConditionContext, mockAnnotatedTypeMetadata);

    assertTrue(result);
  }

  @Test
  public void testMatchesWithEmptyArgs() {
    List<String> nonOptionArgs = Collections.emptyList();
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    boolean result = sqlSetupCondition.matches(mockConditionContext, mockAnnotatedTypeMetadata);

    assertFalse(result);
  }

  @Test
  public void testMatchesWithNullArgs() {
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(null);

    boolean result = sqlSetupCondition.matches(mockConditionContext, mockAnnotatedTypeMetadata);

    assertFalse(result);
  }

  @Test
  public void testMatchesWithNullElements() {
    List<String> nonOptionArgs = Arrays.asList("LoadIndices", null, "SqlSetup");
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    boolean result = sqlSetupCondition.matches(mockConditionContext, mockAnnotatedTypeMetadata);

    assertTrue(result);
  }

  @Test
  public void testMatchesWithCaseSensitivity() {
    List<String> nonOptionArgs = Arrays.asList("sqlsetup", "SQLSETUP", "SqlSetup");
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    boolean result = sqlSetupCondition.matches(mockConditionContext, mockAnnotatedTypeMetadata);

    // Should match only the exact case "SqlSetup"
    assertTrue(result);
  }

  @Test
  public void testConstants() {
    assertEquals(SqlSetupCondition.SQL_SETUP_ARG, "SqlSetup");
    assertTrue(SqlSetupCondition.SQL_SETUP_ARGS.contains("SqlSetup"));
    assertEquals(SqlSetupCondition.SQL_SETUP_ARGS.size(), 1);
  }

  @Test
  public void testMatchesWithMultipleSqlSetupArgs() {
    List<String> nonOptionArgs = Arrays.asList("SqlSetup", "SqlSetup", "LoadIndices");
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    boolean result = sqlSetupCondition.matches(mockConditionContext, mockAnnotatedTypeMetadata);

    assertTrue(result);
  }

  @Test
  public void testMatchesWithWhitespaceArgs() {
    List<String> nonOptionArgs = Arrays.asList("  SqlSetup  ", "LoadIndices");
    when(mockApplicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    boolean result = sqlSetupCondition.matches(mockConditionContext, mockAnnotatedTypeMetadata);

    // Should not match due to whitespace
    assertFalse(result);
  }
}

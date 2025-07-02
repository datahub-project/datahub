package com.linkedin.datahub.upgrade.config;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ReindexDebugConditionTest {

  @Mock private ConditionContext conditionContext;

  @Mock private AnnotatedTypeMetadata annotatedTypeMetadata;

  @Mock private ConfigurableListableBeanFactory beanFactory;

  @Mock private ApplicationArguments applicationArguments;

  private ReindexDebugCondition reindexDebugCondition;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    reindexDebugCondition = new ReindexDebugCondition();

    // Setup the basic mock chain
    when(conditionContext.getBeanFactory()).thenReturn(beanFactory);
    when(beanFactory.getBean(ApplicationArguments.class)).thenReturn(applicationArguments);
  }

  @Test
  public void testConstant() {
    // Verify the constant is correctly defined
    assertEquals(ReindexDebugCondition.DEBUG_REINDEX, "ReindexDebug");
  }

  @Test
  public void testMatches_WithExactMatch() {
    // Arrange
    List<String> nonOptionArgs = Arrays.asList("ReindexDebug", "otherArg");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertTrue(result);
    verify(conditionContext).getBeanFactory();
    verify(beanFactory).getBean(ApplicationArguments.class);
    verify(applicationArguments).getNonOptionArgs();
  }

  @DataProvider(name = "caseInsensitiveArguments")
  public Object[][] provideCaseInsensitiveArguments() {
    return new Object[][] {
      {"reindexdebug"},
      {"REINDEXDEBUG"},
      {"ReindexDebug"},
      {"reindexDebug"},
      {"REINDEXdebug"},
      {"ReInDeXdEbUg"}
    };
  }

  @Test(dataProvider = "caseInsensitiveArguments")
  public void testMatches_WithCaseInsensitiveMatch(String argument) {
    // Arrange
    List<String> nonOptionArgs = Arrays.asList(argument, "otherArg");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertTrue(result, "Should match case-insensitive argument: " + argument);
  }

  @Test
  public void testMatches_WithNoMatch() {
    // Arrange
    List<String> nonOptionArgs = Arrays.asList("someOtherArg", "anotherArg");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertFalse(result);
  }

  @Test
  public void testMatches_WithEmptyArguments() {
    // Arrange
    List<String> nonOptionArgs = Collections.emptyList();
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertFalse(result);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testMatches_WithNullArguments() {
    // Arrange - ApplicationArguments returns null list
    when(applicationArguments.getNonOptionArgs()).thenReturn(null);

    // Act & Assert
    reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);
  }

  @Test
  public void testMatches_WithNullElementsInList() {
    // Arrange
    List<String> nonOptionArgs = Arrays.asList("validArg", null, "ReindexDebug", null);
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertTrue(result, "Should filter out null elements and still find match");
  }

  @Test
  public void testMatches_WithOnlyNullElements() {
    // Arrange
    List<String> nonOptionArgs = Arrays.asList(null, null, null);
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertFalse(result, "Should return false when all elements are null");
  }

  @DataProvider(name = "argumentListsWithMatches")
  public Object[][] provideArgumentListsWithMatches() {
    return new Object[][] {
      {Arrays.asList("ReindexDebug"), true},
      {Arrays.asList("reindexdebug"), true},
      {Arrays.asList("start", "ReindexDebug", "config"), true},
      {Arrays.asList("ReindexDebug", "start"), true},
      {Arrays.asList("config", "reindexdebug"), true},
      {Arrays.asList("Reindex", "Debug"), false},
      {Arrays.asList("ReindexDebugging"), false},
      {Arrays.asList("ReindexDebu"), false},
      {Arrays.asList("start", "config", "run"), false},
      {Arrays.asList(""), false},
      {Arrays.asList(" ReindexDebug "), false}, // with spaces
      {Collections.singletonList("ReindexDebug"), true},
      {Collections.emptyList(), false}
    };
  }

  @Test(dataProvider = "argumentListsWithMatches")
  public void testMatches_WithVariousArgumentCombinations(
      List<String> arguments, boolean expectedResult) {
    // Arrange
    when(applicationArguments.getNonOptionArgs()).thenReturn(arguments);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertEquals(
        result,
        expectedResult,
        "Failed for arguments: " + arguments + ", expected: " + expectedResult);
  }

  @Test
  public void testMatches_WithSingleMatchingArgument() {
    // Arrange
    List<String> nonOptionArgs = Collections.singletonList("REINDEXDEBUG");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertTrue(result);
  }

  @Test
  public void testMatches_WithMultipleMatchingArguments() {
    // Arrange - Multiple matches should still return true
    List<String> nonOptionArgs = Arrays.asList("ReindexDebug", "reindexdebug", "REINDEXDEBUG");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertTrue(result);
  }

  @Test
  public void testMatches_WithSimilarButNotExactArguments() {
    // Arrange
    List<String> nonOptionArgs =
        Arrays.asList(
            "ReindexDebugger",
            "ReindexDebugging",
            "Reindex-Debug",
            "Reindex_Debug",
            "ReindexDebugMode");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertFalse(result, "Should not match similar but not exact strings");
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = "Bean factory error")
  public void testMatches_BeanFactoryThrowsException() {
    // Arrange
    when(conditionContext.getBeanFactory()).thenThrow(new RuntimeException("Bean factory error"));

    // Act & Assert
    reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = "Bean not found")
  public void testMatches_GetBeanThrowsException() {
    // Arrange
    when(beanFactory.getBean(ApplicationArguments.class))
        .thenThrow(new RuntimeException("Bean not found"));

    // Act & Assert
    reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);
  }

  @Test
  public void testMatches_VerifyMethodCallOrder() {
    // Arrange
    List<String> nonOptionArgs = Arrays.asList("ReindexDebug");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertTrue(result);

    // Verify the method call order
    verify(conditionContext, times(1)).getBeanFactory();
    verify(beanFactory, times(1)).getBean(ApplicationArguments.class);
    verify(applicationArguments, times(1)).getNonOptionArgs();

    // Verify no other interactions
    verifyNoMoreInteractions(conditionContext, beanFactory, applicationArguments);
    verifyNoInteractions(annotatedTypeMetadata);
  }

  @Test
  public void testMatches_WithLargeArgumentList() {
    // Arrange - Test performance with many arguments
    List<String> nonOptionArgs =
        Arrays.asList(
            "arg1",
            "arg2",
            "arg3",
            "arg4",
            "arg5",
            "arg6",
            "arg7",
            "arg8",
            "arg9",
            "arg10",
            "arg11",
            "arg12",
            "arg13",
            "arg14",
            "arg15",
            "arg16",
            "arg17",
            "arg18",
            "arg19",
            "arg20",
            "ReindexDebug", // Match in middle
            "arg21",
            "arg22",
            "arg23",
            "arg24",
            "arg25");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertTrue(result, "Should find match even in large argument list");
  }

  @Test
  public void testMatches_WithSpecialCharactersInArguments() {
    // Arrange
    List<String> nonOptionArgs =
        Arrays.asList(
            "arg-with-dashes",
            "arg_with_underscores",
            "arg.with.dots",
            "arg@with@symbols",
            "ReindexDebug",
            "arg with spaces");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertTrue(result);
  }

  @Test
  public void testImplementsConditionInterface() {
    // Verify that the class properly implements the Condition interface
    assertTrue(reindexDebugCondition instanceof org.springframework.context.annotation.Condition);
  }

  @Test
  public void testConstantAccessibility() {
    // Verify the constant can be accessed statically
    String debugConstant = ReindexDebugCondition.DEBUG_REINDEX;
    assertNotNull(debugConstant);
    assertFalse(debugConstant.isEmpty());
    assertEquals(debugConstant, "ReindexDebug");
  }

  @Test
  public void testMatches_MetadataParameterNotUsed() {
    // Arrange
    List<String> nonOptionArgs = Arrays.asList("ReindexDebug");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act - Call with null metadata to ensure it's not used
    boolean result = reindexDebugCondition.matches(conditionContext, null);

    // Assert
    assertTrue(result);
    // Verify metadata was never accessed (no interactions)
    verifyNoInteractions(annotatedTypeMetadata);
  }
}

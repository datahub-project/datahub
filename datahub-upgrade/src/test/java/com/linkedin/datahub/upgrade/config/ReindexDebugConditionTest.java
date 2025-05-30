package com.linkedin.datahub.upgrade.config;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class ReindexDebugConditionTest {

  @Mock private ConditionContext conditionContext;

  @Mock private AnnotatedTypeMetadata annotatedTypeMetadata;

  @Mock private BeanFactory beanFactory;

  @Mock private ApplicationArguments applicationArguments;

  private ReindexDebugCondition reindexDebugCondition;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    reindexDebugCondition = new ReindexDebugCondition();

    // Setup the basic mock chain
    when(conditionContext.getBeanFactory())
        .thenReturn((ConfigurableListableBeanFactory) beanFactory);
    when(beanFactory.getBean(ApplicationArguments.class)).thenReturn(applicationArguments);
  }

  @Test
  void testConstant() {
    // Verify the constant is correctly defined
    assertEquals("ReindexDebug", ReindexDebugCondition.DEBUG_REINDEX);
  }

  @Test
  void testMatches_WithExactMatch() {
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

  @ParameterizedTest
  @ValueSource(
      strings = {
        "reindexdebug",
        "REINDEXDEBUG",
        "ReindexDebug",
        "reindexDebug",
        "REINDEXdebug",
        "ReInDeXdEbUg"
      })
  void testMatches_WithCaseInsensitiveMatch(String argument) {
    // Arrange
    List<String> nonOptionArgs = Arrays.asList(argument, "otherArg");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertTrue(result, "Should match case-insensitive argument: " + argument);
  }

  @Test
  void testMatches_WithNoMatch() {
    // Arrange
    List<String> nonOptionArgs = Arrays.asList("someOtherArg", "anotherArg");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertFalse(result);
  }

  @Test
  void testMatches_WithEmptyArguments() {
    // Arrange
    List<String> nonOptionArgs = Collections.emptyList();
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertFalse(result);
  }

  @Test
  void testMatches_WithNullArguments() {
    // Arrange - ApplicationArguments returns null list
    when(applicationArguments.getNonOptionArgs()).thenReturn(null);

    // Act & Assert
    assertThrows(
        NullPointerException.class,
        () -> reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata));
  }

  @Test
  void testMatches_WithNullElementsInList() {
    // Arrange
    List<String> nonOptionArgs = Arrays.asList("validArg", null, "ReindexDebug", null);
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertTrue(result, "Should filter out null elements and still find match");
  }

  @Test
  void testMatches_WithOnlyNullElements() {
    // Arrange
    List<String> nonOptionArgs = Arrays.asList(null, null, null);
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertFalse(result, "Should return false when all elements are null");
  }

  @ParameterizedTest
  @MethodSource("provideArgumentListsWithMatches")
  void testMatches_WithVariousArgumentCombinations(List<String> arguments, boolean expectedResult) {
    // Arrange
    when(applicationArguments.getNonOptionArgs()).thenReturn(arguments);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertEquals(
        expectedResult,
        result,
        "Failed for arguments: " + arguments + ", expected: " + expectedResult);
  }

  static Stream<Arguments> provideArgumentListsWithMatches() {
    return Stream.of(
        Arguments.of(Arrays.asList("ReindexDebug"), true),
        Arguments.of(Arrays.asList("reindexdebug"), true),
        Arguments.of(Arrays.asList("start", "ReindexDebug", "config"), true),
        Arguments.of(Arrays.asList("ReindexDebug", "start"), true),
        Arguments.of(Arrays.asList("config", "reindexdebug"), true),
        Arguments.of(Arrays.asList("Reindex", "Debug"), false),
        Arguments.of(Arrays.asList("ReindexDebugging"), false),
        Arguments.of(Arrays.asList("ReindexDebu"), false),
        Arguments.of(Arrays.asList("start", "config", "run"), false),
        Arguments.of(Arrays.asList(""), false),
        Arguments.of(Arrays.asList(" ReindexDebug "), false), // with spaces
        Arguments.of(Collections.singletonList("ReindexDebug"), true),
        Arguments.of(Collections.emptyList(), false));
  }

  @Test
  void testMatches_WithSingleMatchingArgument() {
    // Arrange
    List<String> nonOptionArgs = Collections.singletonList("REINDEXDEBUG");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertTrue(result);
  }

  @Test
  void testMatches_WithMultipleMatchingArguments() {
    // Arrange - Multiple matches should still return true
    List<String> nonOptionArgs = Arrays.asList("ReindexDebug", "reindexdebug", "REINDEXDEBUG");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertTrue(result);
  }

  @Test
  void testMatches_WithSimilarButNotExactArguments() {
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

  @Test
  void testMatches_BeanFactoryThrowsException() {
    // Arrange
    when(conditionContext.getBeanFactory()).thenThrow(new RuntimeException("Bean factory error"));

    // Act & Assert
    assertThrows(
        RuntimeException.class,
        () -> reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata));
  }

  @Test
  void testMatches_GetBeanThrowsException() {
    // Arrange
    when(beanFactory.getBean(ApplicationArguments.class))
        .thenThrow(new RuntimeException("Bean not found"));

    // Act & Assert
    assertThrows(
        RuntimeException.class,
        () -> reindexDebugCondition.matches(conditionContext, annotatedTypeMetadata));
  }

  @Test
  void testMatches_VerifyMethodCallOrder() {
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
  void testMatches_WithLargeArgumentList() {
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
  void testMatches_WithSpecialCharactersInArguments() {
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
  void testImplementsConditionInterface() {
    // Verify that the class properly implements the Condition interface
    assertTrue(reindexDebugCondition instanceof org.springframework.context.annotation.Condition);
  }

  @Test
  void testConstantAccessibility() {
    // Verify the constant can be accessed statically
    String debugConstant = ReindexDebugCondition.DEBUG_REINDEX;
    assertNotNull(debugConstant);
    assertFalse(debugConstant.isEmpty());
    assertEquals("ReindexDebug", debugConstant);
  }

  @Test
  void testMatches_MetadataParameterNotUsed() {
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

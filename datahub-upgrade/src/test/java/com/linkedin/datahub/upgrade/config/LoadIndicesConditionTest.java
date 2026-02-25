package com.linkedin.datahub.upgrade.config;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.datahub.upgrade.conditions.LoadIndicesCondition;
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

public class LoadIndicesConditionTest {

  @Mock private ConditionContext conditionContext;

  @Mock private AnnotatedTypeMetadata annotatedTypeMetadata;

  @Mock private ConfigurableListableBeanFactory beanFactory;

  @Mock private ApplicationArguments applicationArguments;

  private LoadIndicesCondition loadIndicesCondition;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    loadIndicesCondition = new LoadIndicesCondition();

    // Setup the basic mock chain
    when(conditionContext.getBeanFactory()).thenReturn(beanFactory);
    when(beanFactory.getBean(ApplicationArguments.class)).thenReturn(applicationArguments);
  }

  @Test
  public void testConstant() {
    // Verify the constant is correctly defined
    assertEquals(LoadIndicesCondition.LOAD_INDICES_ARG, "LoadIndices");
  }

  @Test
  public void testMatches_WithExactMatch() {
    // Arrange
    List<String> nonOptionArgs = Arrays.asList("LoadIndices", "otherArg");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = loadIndicesCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertTrue(result);
    verify(conditionContext).getBeanFactory();
    verify(beanFactory).getBean(ApplicationArguments.class);
    verify(applicationArguments).getNonOptionArgs();
  }

  @Test
  public void testMatches_WithNoMatch() {
    // Arrange
    List<String> nonOptionArgs = Arrays.asList("someOtherArg", "anotherArg");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = loadIndicesCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertFalse(result);
  }

  @Test
  public void testMatches_WithEmptyArguments() {
    // Arrange
    List<String> nonOptionArgs = Collections.emptyList();
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = loadIndicesCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertFalse(result);
  }

  @Test
  public void testMatches_WithNullArguments() {
    // Arrange
    when(applicationArguments.getNonOptionArgs()).thenReturn(null);

    // Act
    boolean result = loadIndicesCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertFalse(result);
  }

  @Test
  public void testMatches_WithNullElementsInArguments() {
    // Arrange
    List<String> nonOptionArgs = Arrays.asList("LoadIndices", null, "otherArg");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = loadIndicesCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertTrue(result);
  }

  @Test
  public void testMatches_WithOnlyNullElements() {
    // Arrange
    List<String> nonOptionArgs = Arrays.asList(null, null);
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = loadIndicesCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertFalse(result);
  }

  @Test
  public void testMatches_WithLoadIndicesOnly() {
    // Arrange
    List<String> nonOptionArgs = Arrays.asList("LoadIndices");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = loadIndicesCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertTrue(result);
  }

  @Test
  public void testMatches_WithLoadIndicesAtEnd() {
    // Arrange
    List<String> nonOptionArgs = Arrays.asList("arg1", "arg2", "LoadIndices");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = loadIndicesCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertTrue(result);
  }

  @Test
  public void testMatches_WithLoadIndicesAtBeginning() {
    // Arrange
    List<String> nonOptionArgs = Arrays.asList("LoadIndices", "arg1", "arg2");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = loadIndicesCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertTrue(result);
  }

  @Test
  public void testMatches_WithLoadIndicesInMiddle() {
    // Arrange
    List<String> nonOptionArgs = Arrays.asList("arg1", "LoadIndices", "arg2");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = loadIndicesCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertTrue(result);
  }

  @Test
  public void testMatches_WithLargeArgumentList() {
    // Arrange
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
            "LoadIndices",
            "arg11",
            "arg12",
            "arg13",
            "arg14",
            "arg15");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = loadIndicesCondition.matches(conditionContext, annotatedTypeMetadata);

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
            "LoadIndices",
            "arg with spaces");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act
    boolean result = loadIndicesCondition.matches(conditionContext, annotatedTypeMetadata);

    // Assert
    assertTrue(result);
  }

  @Test
  public void testImplementsConditionInterface() {
    // Verify that the class properly implements the Condition interface
    assertTrue(loadIndicesCondition instanceof org.springframework.context.annotation.Condition);
  }

  @Test
  public void testConstantAccessibility() {
    // Verify the constant can be accessed statically
    String loadIndicesConstant = LoadIndicesCondition.LOAD_INDICES_ARG;
    assertNotNull(loadIndicesConstant);
    assertFalse(loadIndicesConstant.isEmpty());
    assertEquals(loadIndicesConstant, "LoadIndices");
  }

  @Test
  public void testMatches_MetadataParameterNotUsed() {
    // Arrange
    List<String> nonOptionArgs = Arrays.asList("LoadIndices");
    when(applicationArguments.getNonOptionArgs()).thenReturn(nonOptionArgs);

    // Act - Call with null metadata to ensure it's not used
    boolean result = loadIndicesCondition.matches(conditionContext, null);

    // Assert
    assertTrue(result);
    // Verify metadata was never accessed (no interactions)
    verifyNoInteractions(annotatedTypeMetadata);
  }

  @Test
  public void testLoadIndicesArgsSet() {
    // Verify the LOAD_INDICES_ARGS set contains the expected value
    assertTrue(LoadIndicesCondition.LOAD_INDICES_ARGS.contains("LoadIndices"));
    assertEquals(LoadIndicesCondition.LOAD_INDICES_ARGS.size(), 1);
  }
}

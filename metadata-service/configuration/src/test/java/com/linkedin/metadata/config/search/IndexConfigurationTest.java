package com.linkedin.metadata.config.search;

import org.testng.Assert;
import org.testng.annotations.Test;

public class IndexConfigurationTest {

  @Test
  public void testGetFinalPrefix_WithPrefix() {
    // Arrange
    IndexConfiguration config = new IndexConfiguration();
    config.setPrefix("prod");

    // Act
    String result = config.getFinalPrefix();

    // Assert
    Assert.assertEquals(result, "prod_");
  }

  @Test
  public void testGetFinalPrefix_EmptyPrefix() {
    // Arrange
    IndexConfiguration config = new IndexConfiguration();
    config.setPrefix("");

    // Act
    String result = config.getFinalPrefix();

    // Assert
    Assert.assertEquals(result, "");
  }

  @Test
  public void testGetFinalPrefix_NullPrefix() {
    // Arrange
    IndexConfiguration config = new IndexConfiguration();
    config.setPrefix(null);

    // Act
    String result = config.getFinalPrefix();

    // Assert
    Assert.assertEquals(result, "");
  }

  @Test
  public void testGetFinalPrefix_DefaultConstructor() {
    // Arrange
    IndexConfiguration config = new IndexConfiguration();
    // prefix is null by default

    // Act
    String result = config.getFinalPrefix();

    // Assert
    Assert.assertEquals(result, "");
  }

  @Test
  public void testGetFinalPrefix_WithComplexPrefix() {
    // Arrange
    IndexConfiguration config = new IndexConfiguration();
    config.setPrefix("kbcpyv7ss3-staging-test");

    // Act
    String result = config.getFinalPrefix();

    // Assert
    Assert.assertEquals(result, "kbcpyv7ss3-staging-test_");
  }

  @Test
  public void testGetFinalPrefix_WithWhitespacePrefix() {
    // Arrange
    IndexConfiguration config = new IndexConfiguration();
    config.setPrefix("   ");

    // Act
    String result = config.getFinalPrefix();

    // Assert
    // Whitespace-only string is not considered empty by isEmpty()
    Assert.assertEquals(result, "   _");
  }
}

package com.linkedin.metadata.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class UrnValidationUtilTest {
  private static final EntityRegistry entityRegistry =
      TestOperationContexts.defaultEntityRegistry();

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testUrnWithTrailingWhitespace() {
    Urn invalidUrn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD) ");
    UrnValidationUtil.validateUrn(entityRegistry, invalidUrn, true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testExcessiveLength() {
    StringBuilder longPath = new StringBuilder("urn:li:dataset:(urn:li:dataPlatform:hdfs,");
    // Create a path that will exceed 512 bytes when URL encoded
    for (int i = 0; i < 500; i++) {
      longPath.append("very/long/path/");
    }
    longPath.append(",PROD)");
    Urn invalidUrn = UrnUtils.getUrn(longPath.toString());

    UrnValidationUtil.validateUrn(entityRegistry, invalidUrn, true);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testUrnNull() {
    UrnValidationUtil.validateUrn(entityRegistry, null, true);
  }

  /**
   * Common method to validate URNs from a file and return the validation results.
   *
   * @param filePath Path to the file containing URNs.
   * @return ValidationResult containing lists of valid and invalid URNs.
   * @throws IOException If there is an error reading the file.
   */
  private ValidationResult validateUrnsFromFile(
      @Nonnull String filePath, @Nonnull Set<String> excludePrefix)
      throws IOException, URISyntaxException {
    List<String> invalidUrns = new ArrayList<>();
    List<String> validUrns = new ArrayList<>();
    int totalUrns = 0;

    File file = new File(filePath);
    BufferedReader reader = new BufferedReader(new FileReader(file));
    String line;

    while ((line = reader.readLine()) != null) {
      // Skip empty lines and comment lines (starting with #)
      line = line.trim();
      final String excludeCheck = line;
      if (line.isEmpty()
          || line.startsWith("#")
          || excludePrefix.stream().anyMatch(excludeCheck::startsWith)) {
        continue;
      }

      totalUrns++;

      try {
        Urn urn = UrnUtils.getUrn(line);
        UrnValidationUtil.validateUrn(entityRegistry, urn, true);
        validUrns.add(line);
      } catch (Exception e) {
        invalidUrns.add(line + " - Error: " + e.getMessage());
      }

      if (validUrns.contains(line)) {
        // If valid should also parse correctly
        Urn.createFromString(line);
      }
    }

    reader.close();

    // Print summary
    System.out.println("File: " + filePath);
    System.out.println("Total URNs processed: " + totalUrns);
    System.out.println("Valid URNs: " + validUrns.size());
    System.out.println("Invalid URNs: " + invalidUrns.size());

    return new ValidationResult(validUrns, invalidUrns, totalUrns);
  }

  /**
   * Test method to validate URNs from a file containing valid URNs. Expects all URNs in the file to
   * be valid.
   *
   * @param filePath Path to the file containing valid URNs.
   */
  @Test(dataProvider = "validUrnFilePathProvider")
  public void testValidateValidUrnsFromFile(String filePath) throws URISyntaxException {
    try {
      ValidationResult result = validateUrnsFromFile(filePath, Set.of("urn:li:abc:"));

      // Print invalid URNs if any exist
      if (!result.getInvalidUrns().isEmpty()) {
        System.out.println("Invalid URNs found in valid URN file:");
        result.getInvalidUrns().forEach(System.out::println);
        Assert.fail("Found " + result.getInvalidUrns().size() + " invalid URNs in valid URN file");
      }

      // Assert that we have at least one test case
      Assert.assertTrue(result.getValidUrns().size() > 0, "No valid URNs found in the file");

    } catch (IOException e) {
      Assert.fail("Failed to read the file: " + e.getMessage());
    }
  }

  /**
   * Test method to validate URNs from a file containing invalid URNs. Expects all URNs in the file
   * to be invalid.
   *
   * @param filePath Path to the file containing invalid URNs.
   */
  @Test(dataProvider = "invalidUrnFilePathProvider")
  public void testValidateInvalidUrnsFromFile(String filePath) throws URISyntaxException {
    try {
      ValidationResult result = validateUrnsFromFile(filePath, Set.of());

      // Print valid URNs if any exist
      if (!result.getValidUrns().isEmpty()) {
        System.out.println("Valid URNs found in invalid URN file:");
        result.getValidUrns().forEach(System.out::println);
        Assert.fail("Found " + result.getValidUrns().size() + " valid URNs in invalid URN file");
      }

      // Assert that we have at least one test case
      Assert.assertTrue(result.getInvalidUrns().size() > 0, "No invalid URNs found in the file");

    } catch (IOException e) {
      Assert.fail("Failed to read the file: " + e.getMessage());
    }
  }

  /**
   * Data provider for the valid URN file paths.
   *
   * @return Array of test data
   */
  @DataProvider(name = "validUrnFilePathProvider")
  public Object[][] validUrnFilePathProvider() {
    return new Object[][] {{"../metadata-ingestion/tests/unit/urns/valid_urns.txt"}
      // Add more test files as needed
    };
  }

  /**
   * Data provider for the invalid URN file paths.
   *
   * @return Array of test data
   */
  @DataProvider(name = "invalidUrnFilePathProvider")
  public Object[][] invalidUrnFilePathProvider() {
    return new Object[][] {
      {"../metadata-ingestion/tests/unit/urns/invalid_urns.txt"},
      {"../metadata-ingestion/tests/unit/urns/invalid_urns_java_only.txt"}
      // Add more test files as needed
    };
  }

  /** Class to hold validation results. */
  private static class ValidationResult {
    private final List<String> validUrns;
    private final List<String> invalidUrns;
    private final int totalUrns;

    public ValidationResult(List<String> validUrns, List<String> invalidUrns, int totalUrns) {
      this.validUrns = validUrns;
      this.invalidUrns = invalidUrns;
      this.totalUrns = totalUrns;
    }

    public List<String> getValidUrns() {
      return validUrns;
    }

    public List<String> getInvalidUrns() {
      return invalidUrns;
    }

    public int getTotalUrns() {
      return totalUrns;
    }
  }
}

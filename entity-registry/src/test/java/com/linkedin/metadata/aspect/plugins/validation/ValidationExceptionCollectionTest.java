package com.linkedin.metadata.aspect.plugins.validation;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.test.TestEntityProfile;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class ValidationExceptionCollectionTest {
  private final Urn TEST_URN = UrnUtils.getUrn("urn:li:chart:123");

  private ValidationExceptionCollection collection;
  private EntityRegistry testEntityRegistry;

  private static final String ERROR_MESSAGE = "Test error message";

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @BeforeMethod
  public void setUp() {
    collection = ValidationExceptionCollection.newCollection();
    testEntityRegistry =
        new ConfigEntityRegistry(
            TestEntityProfile.class
                .getClassLoader()
                .getResourceAsStream("test-entity-registry.yml"));
  }

  @Test
  public void testNewCollection() {
    assertNotNull(collection);
    assertTrue(collection.isEmpty());
    assertFalse(collection.hasFatalExceptions());
    assertEquals(collection.getSubTypes().size(), 0);
  }

  @Test
  public void testAddException() {
    BatchItem testItem =
        TestMCP.ofOneMCP(TEST_URN, new Status(), testEntityRegistry).stream().findFirst().get();
    AspectValidationException exception =
        new AspectValidationException(testItem, ERROR_MESSAGE, ValidationSubType.VALIDATION, null);

    collection.addException(exception);

    assertEquals(collection.size(), 1);
    assertTrue(collection.containsKey(exception.getAspectGroup()));
    assertTrue(collection.get(exception.getAspectGroup()).contains(exception));
  }

  @Test
  public void testAddExceptionWithMessage() {
    BatchItem testItem =
        TestMCP.ofOneMCP(TEST_URN, new Status(), testEntityRegistry).stream().findFirst().get();
    collection.addException(testItem, ERROR_MESSAGE);

    assertEquals(collection.size(), 1);
    assertTrue(collection.hasFatalExceptions());
  }

  @Test
  public void testHasFatalExceptionsWithMultipleTypes() {
    BatchItem testItem =
        TestMCP.ofOneMCP(TEST_URN, new Status(), testEntityRegistry).stream().findFirst().get();

    // Add FILTER exception
    collection.addException(
        new AspectValidationException(testItem, ERROR_MESSAGE, ValidationSubType.FILTER, null));
    assertFalse(collection.hasFatalExceptions());

    // Add VALIDATION exception
    collection.addException(
        new AspectValidationException(testItem, ERROR_MESSAGE, ValidationSubType.VALIDATION, null));
    assertTrue(collection.hasFatalExceptions());
  }

  @Test
  public void testGetSubTypesWithAllTypes() {
    BatchItem testItem =
        TestMCP.ofOneMCP(TEST_URN, new Status(), testEntityRegistry).stream().findFirst().get();

    collection.addException(
        new AspectValidationException(testItem, ERROR_MESSAGE, ValidationSubType.FILTER, null));
    collection.addException(
        new AspectValidationException(testItem, ERROR_MESSAGE, ValidationSubType.VALIDATION, null));
    collection.addException(
        new AspectValidationException(
            testItem, ERROR_MESSAGE, ValidationSubType.PRECONDITION, null));

    Set<ValidationSubType> subTypes = collection.getSubTypes();
    assertEquals(subTypes.size(), 3);
    assertTrue(
        subTypes.containsAll(
            Arrays.asList(
                ValidationSubType.FILTER,
                ValidationSubType.VALIDATION,
                ValidationSubType.PRECONDITION)));
  }

  @Test
  public void testSuccessfulAndExceptionItems() {
    BatchItem validationItem =
        TestMCP.ofOneMCP(UrnUtils.getUrn("urn:li:chart:111"), new Status(), testEntityRegistry)
            .stream()
            .findFirst()
            .get();
    BatchItem filterItem =
        TestMCP.ofOneMCP(UrnUtils.getUrn("urn:li:chart:222"), new Status(), testEntityRegistry)
            .stream()
            .findFirst()
            .get();
    BatchItem successItem =
        TestMCP.ofOneMCP(UrnUtils.getUrn("urn:li:chart:333"), new Status(), testEntityRegistry)
            .stream()
            .findFirst()
            .get();

    collection.addException(
        new AspectValidationException(
            validationItem, ERROR_MESSAGE, ValidationSubType.VALIDATION, null));
    collection.addException(
        new AspectValidationException(filterItem, ERROR_MESSAGE, ValidationSubType.FILTER, null));

    Collection<BatchItem> items = Arrays.asList(validationItem, filterItem, successItem);

    // Test successful items
    Collection<BatchItem> successful = collection.successful(items);
    assertEquals(successful.size(), 1);
    assertTrue(successful.contains(successItem));

    // Test exception items
    Collection<BatchItem> exceptions = collection.exceptions(items);
    assertEquals(exceptions.size(), 1);
    assertTrue(exceptions.contains(validationItem));
    assertFalse(exceptions.contains(filterItem)); // FILTER type should not be included
  }

  @Test
  public void testStreamOperations() {
    BatchItem validationItem =
        TestMCP.ofOneMCP(UrnUtils.getUrn("urn:li:chart:111"), new Status(), testEntityRegistry)
            .stream()
            .findFirst()
            .get();
    BatchItem successItem =
        TestMCP.ofOneMCP(UrnUtils.getUrn("urn:li:chart:222"), new Status(), testEntityRegistry)
            .stream()
            .findFirst()
            .get();

    collection.addException(
        new AspectValidationException(
            validationItem, ERROR_MESSAGE, ValidationSubType.VALIDATION, null));

    List<BatchItem> items = Arrays.asList(validationItem, successItem);

    // Test streamSuccessful
    List<BatchItem> successful = collection.streamSuccessful(items.stream()).toList();
    assertEquals(successful.size(), 1);
    assertTrue(successful.contains(successItem));

    // Test streamExceptions
    List<BatchItem> exceptions = collection.streamExceptions(items.stream()).toList();
    assertEquals(exceptions.size(), 1);
    assertTrue(exceptions.contains(validationItem));
  }

  @Test
  public void testMultipleExceptionsForSameEntityDifferentAspects() {
    BatchItem item1 =
        TestMCP.ofOneMCP(UrnUtils.getUrn("urn:li:chart:111"), new Status(), testEntityRegistry)
            .stream()
            .findFirst()
            .get();
    BatchItem item2 =
        TestMCP.ofOneMCP(UrnUtils.getUrn("urn:li:chart:222"), new Status(), testEntityRegistry)
            .stream()
            .findFirst()
            .get();

    collection.addException(
        new AspectValidationException(item1, ERROR_MESSAGE, ValidationSubType.VALIDATION, null));
    collection.addException(
        new AspectValidationException(item2, ERROR_MESSAGE, ValidationSubType.VALIDATION, null));

    assertEquals(collection.size(), 2);
    assertEquals(collection.getSubTypes().size(), 1);
  }

  @Test
  public void testToString() {
    BatchItem testItem =
        TestMCP.ofOneMCP(TEST_URN, new Status(), testEntityRegistry).stream().findFirst().get();
    AspectValidationException exception =
        new AspectValidationException(testItem, ERROR_MESSAGE, ValidationSubType.VALIDATION, null);

    collection.addException(exception);

    String result = collection.toString();
    assertTrue(result.contains("ValidationExceptionCollection"));
    assertTrue(result.contains("EntityAspect:"));
    assertTrue(result.contains("urn:li:chart:123"));
  }

  @Test
  public void testAddAuthException() {
    BatchItem testItem =
        TestMCP.ofOneMCP(TEST_URN, new Status(), testEntityRegistry).stream().findFirst().get();
    String authErrorMessage = "Authorization failed for user";

    collection.addAuthException(testItem, authErrorMessage);

    // Verify the exception was added
    assertEquals(collection.size(), 1);

    // Verify it's an AUTHORIZATION subtype
    assertTrue(collection.getSubTypes().contains(ValidationSubType.AUTHORIZATION));

    // Verify it's a fatal exception (AUTHORIZATION is not FILTER type)
    assertTrue(collection.hasFatalExceptions());

    // Get the actual exception and verify its properties
    AspectValidationException authException =
        collection.values().stream().flatMap(Collection::stream).findFirst().orElse(null);

    assertNotNull(authException);
    assertEquals(authException.getSubType(), ValidationSubType.AUTHORIZATION);
    assertEquals(authException.getMsg(), authErrorMessage);
    assertEquals(authException.getItem(), testItem);
  }

  @Test
  public void testAddAuthExceptionWithMultipleItems() {
    BatchItem item1 =
        TestMCP.ofOneMCP(UrnUtils.getUrn("urn:li:chart:111"), new Status(), testEntityRegistry)
            .stream()
            .findFirst()
            .get();
    BatchItem item2 =
        TestMCP.ofOneMCP(UrnUtils.getUrn("urn:li:chart:222"), new Status(), testEntityRegistry)
            .stream()
            .findFirst()
            .get();

    collection.addAuthException(item1, "Auth failed for item1");
    collection.addAuthException(item2, "Auth failed for item2");

    // Should have 2 different aspect groups
    assertEquals(collection.size(), 2);

    // All should be AUTHORIZATION type
    assertEquals(collection.getSubTypes().size(), 1);
    assertTrue(collection.getSubTypes().contains(ValidationSubType.AUTHORIZATION));

    // Should be fatal since AUTHORIZATION is not FILTER
    assertTrue(collection.hasFatalExceptions());
  }

  @Test
  public void testMixedExceptionTypes() {
    BatchItem authItem =
        TestMCP.ofOneMCP(UrnUtils.getUrn("urn:li:chart:111"), new Status(), testEntityRegistry)
            .stream()
            .findFirst()
            .get();
    BatchItem validationItem =
        TestMCP.ofOneMCP(UrnUtils.getUrn("urn:li:chart:222"), new Status(), testEntityRegistry)
            .stream()
            .findFirst()
            .get();
    BatchItem filterItem =
        TestMCP.ofOneMCP(UrnUtils.getUrn("urn:li:chart:333"), new Status(), testEntityRegistry)
            .stream()
            .findFirst()
            .get();

    // Add different types of exceptions
    collection.addAuthException(authItem, "Authorization failed");
    collection.addException(validationItem, "Validation failed");
    collection.addException(
        new AspectValidationException(filterItem, "Filter failed", ValidationSubType.FILTER, null));

    // Should have all 3 subtypes
    Set<ValidationSubType> subTypes = collection.getSubTypes();
    assertEquals(subTypes.size(), 3);
    assertTrue(subTypes.contains(ValidationSubType.AUTHORIZATION));
    assertTrue(subTypes.contains(ValidationSubType.VALIDATION));
    assertTrue(subTypes.contains(ValidationSubType.FILTER));

    // Should be fatal because of AUTHORIZATION and VALIDATION
    assertTrue(collection.hasFatalExceptions());

    // Test exceptions method - should not include FILTER items
    Collection<BatchItem> items = Arrays.asList(authItem, validationItem, filterItem);
    Collection<BatchItem> exceptions = collection.exceptions(items);
    assertEquals(exceptions.size(), 2);
    assertTrue(exceptions.contains(authItem));
    assertTrue(exceptions.contains(validationItem));
    assertFalse(exceptions.contains(filterItem));
  }

  @Test
  public void testAuthExceptionInStreamOperations() {
    BatchItem authItem =
        TestMCP.ofOneMCP(UrnUtils.getUrn("urn:li:chart:111"), new Status(), testEntityRegistry)
            .stream()
            .findFirst()
            .get();
    BatchItem successItem =
        TestMCP.ofOneMCP(UrnUtils.getUrn("urn:li:chart:222"), new Status(), testEntityRegistry)
            .stream()
            .findFirst()
            .get();

    collection.addAuthException(authItem, "Not authorized");

    List<BatchItem> items = Arrays.asList(authItem, successItem);

    // Test streamSuccessful - should only contain successItem
    List<BatchItem> successful = collection.streamSuccessful(items.stream()).toList();
    assertEquals(successful.size(), 1);
    assertTrue(successful.contains(successItem));
    assertFalse(successful.contains(authItem));

    // Test streamExceptions - should contain authItem
    List<BatchItem> exceptions = collection.streamExceptions(items.stream()).toList();
    assertEquals(exceptions.size(), 1);
    assertTrue(exceptions.contains(authItem));
    assertFalse(exceptions.contains(successItem));
  }
}

package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.util.Arrays;
import java.util.Collections;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** TestNG tests for DatasetService, focusing on the public methods. */
public class DatasetServiceTest {

  @Mock private SystemEntityClient mockEntityClient;

  @Mock private OpenApiClient mockOpenApiClient;

  @Mock private ObjectMapper mockObjectMapper;

  @InjectMocks private DatasetService datasetService;

  private Urn datasetUrn;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    datasetService = new DatasetService(mockEntityClient, mockOpenApiClient, mockObjectMapper);

    datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,testDataset,PROD)");
    opContext = mock(OperationContext.class);
  }

  @Test
  public void testSchemaFieldExistsValidDatasetFieldExists() throws Exception {
    // GIVEN
    String fieldPath = "myField";

    // Mock entityClient to say the dataset exists
    when(mockEntityClient.exists(opContext, datasetUrn)).thenReturn(true);

    // Prepare a SchemaMetadata with a field named "myField"
    SchemaField schemaField = new SchemaField();
    schemaField.setFieldPath(fieldPath);
    SchemaMetadata schemaMetadata = new SchemaMetadata();
    schemaMetadata.setFields(new SchemaFieldArray(Arrays.asList(schemaField)));

    // Build an EntityResponse with SCHEMA_METADATA aspect
    Aspect schemaAspect = new Aspect(schemaMetadata.data());
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.SCHEMA_METADATA_ASPECT_NAME,
                new EnvelopedAspect().setValue(schemaAspect))));

    // Mock getV2(...) call
    when(mockEntityClient.getV2(
            eq(opContext),
            eq(Constants.DATASET_ENTITY_NAME),
            eq(datasetUrn),
            eq(ImmutableSet.of(Constants.SCHEMA_METADATA_ASPECT_NAME))))
        .thenReturn(entityResponse);

    // WHEN
    boolean result = datasetService.schemaFieldExists(opContext, datasetUrn, fieldPath);

    // THEN
    Assert.assertTrue(result);
    verify(mockEntityClient).exists(opContext, datasetUrn);
    verify(mockEntityClient)
        .getV2(
            opContext,
            Constants.DATASET_ENTITY_NAME,
            datasetUrn,
            ImmutableSet.of(Constants.SCHEMA_METADATA_ASPECT_NAME));
  }

  @Test
  public void testSchemaFieldExistsValidDatasetFieldNotExists() throws Exception {
    // GIVEN
    String fieldPath = "otherField";

    when(mockEntityClient.exists(opContext, datasetUrn)).thenReturn(true);

    // Build a SchemaMetadata with NO matching fields
    SchemaField schemaField = new SchemaField();
    schemaField.setFieldPath("unrelatedField");
    SchemaMetadata schemaMetadata = new SchemaMetadata();
    schemaMetadata.setFields(new SchemaFieldArray(Collections.singletonList(schemaField)));

    // EntityResponse with that schema
    Aspect schemaAspect = new Aspect(schemaMetadata.data());
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.SCHEMA_METADATA_ASPECT_NAME,
                new EnvelopedAspect().setValue(schemaAspect))));

    when(mockEntityClient.getV2(
            eq(opContext),
            eq(Constants.DATASET_ENTITY_NAME),
            eq(datasetUrn),
            eq(ImmutableSet.of(Constants.SCHEMA_METADATA_ASPECT_NAME))))
        .thenReturn(entityResponse);

    // WHEN
    boolean result = datasetService.schemaFieldExists(opContext, datasetUrn, fieldPath);

    // THEN
    Assert.assertFalse(result);
  }

  @Test
  public void testSchemaFieldExistsInvalidEntityType() {
    // GIVEN
    // Create a URN with a non-dataset entity type
    Urn notADatasetUrn = UrnUtils.getUrn("urn:li:tag:123");
    String fieldPath = "myField";

    // WHEN & THEN
    try {
      datasetService.schemaFieldExists(opContext, notADatasetUrn, fieldPath);
      Assert.fail("Expected InvalidEntityTypeException to be thrown");
    } catch (InvalidEntityTypeException ex) {
      Assert.assertTrue(ex.getMessage().contains("is not a dataset"));
    } catch (Exception ex) {
      Assert.fail("Expected InvalidEntityTypeException but got: " + ex);
    }
  }

  @Test
  public void testSchemaFieldExists_EntityDoesNotExist() throws Exception {
    // GIVEN
    when(mockEntityClient.exists(opContext, datasetUrn)).thenReturn(false);
    String fieldPath = "myField";

    // WHEN & THEN
    try {
      datasetService.schemaFieldExists(opContext, datasetUrn, fieldPath);
      Assert.fail("Expected EntityDoesNotExistException to be thrown");
    } catch (EntityDoesNotExistException ex) {
      Assert.assertTrue(ex.getMessage().contains("does not exist"));
    } catch (Exception ex) {
      Assert.fail("Expected EntityDoesNotExistException but got: " + ex);
    }
  }

  @Test
  public void testSchemaFieldExists_RemoteInvocationException() throws Exception {
    // GIVEN
    // Make entityClient.exists throw a RemoteInvocationException
    when(mockEntityClient.exists(opContext, datasetUrn))
        .thenThrow(new RemoteInvocationException("Downstream service error"));

    String fieldPath = "someField";

    // WHEN & THEN
    try {
      datasetService.schemaFieldExists(opContext, datasetUrn, fieldPath);
      Assert.fail("Expected RemoteInvocationException to be thrown");
    } catch (RemoteInvocationException ex) {
      Assert.assertTrue(ex.getMessage().contains("Downstream service error"));
    } catch (Exception ex) {
      Assert.fail("Expected RemoteInvocationException but got: " + ex);
    }
  }

  @Test
  public void testSchemaFieldExists_NoSchemaMetadata() throws Exception {
    // GIVEN
    when(mockEntityClient.exists(opContext, datasetUrn)).thenReturn(true);

    // Return an EntityResponse with no SCHEMA_METADATA aspect
    EntityResponse noSchemaEntityResponse = new EntityResponse();
    noSchemaEntityResponse.setAspects(new EnvelopedAspectMap()); // empty

    when(mockEntityClient.getV2(
            eq(opContext),
            eq(Constants.DATASET_ENTITY_NAME),
            eq(datasetUrn),
            eq(ImmutableSet.of(Constants.SCHEMA_METADATA_ASPECT_NAME))))
        .thenReturn(noSchemaEntityResponse);

    // WHEN
    boolean result = datasetService.schemaFieldExists(opContext, datasetUrn, "whateverField");

    // THEN
    Assert.assertFalse(result);
  }
}

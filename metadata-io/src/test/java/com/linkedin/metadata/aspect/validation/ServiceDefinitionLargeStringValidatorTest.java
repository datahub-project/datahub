package com.linkedin.metadata.aspect.validation;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.util.RecordUtils;
import com.linkedin.common.CompressionType;
import com.linkedin.common.LargeString;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.utils.LargeStrings;
import com.linkedin.service.ServiceDefinition;
import com.linkedin.service.ServiceDefinitionFormat;
import com.linkedin.test.metadata.aspect.batch.TestPatchMCP;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ServiceDefinitionLargeStringValidatorTest {

  private static final Urn SERVICE_URN = UrnUtils.getUrn("urn:li:service:petstore");

  private ServiceDefinitionLargeStringValidator validator;

  @BeforeMethod
  public void setup() {
    validator = new ServiceDefinitionLargeStringValidator();
  }

  private BatchItem itemFor(final ServiceDefinition definition) {
    final BatchItem item = mock(BatchItem.class);
    when(item.getAspect(ServiceDefinition.class)).thenReturn(definition);
    when(item.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(item.getUrn()).thenReturn(SERVICE_URN);
    when(item.getAspectName()).thenReturn(Constants.SERVICE_DEFINITION_ASPECT_NAME);
    return item;
  }

  private List<AspectValidationException> validate(final ServiceDefinition definition) {
    return validator
        .validateProposedAspects(null, Set.of(itemFor(definition)), null)
        .collect(Collectors.toList());
  }

  @Test
  public void testValidRawSpecPasses() {
    // An encoder-produced rawSpec decodes cleanly -> no violation.
    final ServiceDefinition definition = new ServiceDefinition();
    definition.setFormat(ServiceDefinitionFormat.OPENAPI);
    definition.setRawSpec(LargeStrings.encode("openapi: 3.0.0"));

    assertTrue(validate(definition).isEmpty());
  }

  /** A rawSpec set via patch is decode-checked from the patch's own value at the request stage. */
  @Test
  public void testPatchRawSpecValidated() {
    final String corruptOps =
        "[{\"op\":\"add\",\"path\":\"/rawSpec\",\"value\":"
            + "{\"compression\":\"GZIP\",\"blob\":\"!!!not-valid-base64!!!\"}}]";
    assertEquals(
        validator
            .validateProposedAspects(
                null,
                Set.of(
                    TestPatchMCP.of(
                        SERVICE_URN, Constants.SERVICE_DEFINITION_ASPECT_NAME, corruptOps)),
                null)
            .count(),
        1,
        "Patch with a corrupt rawSpec blob should be rejected");

    final LargeString valid = LargeStrings.encode("openapi: 3.0.0");
    final String validOps =
        "[{\"op\":\"add\",\"path\":\"/rawSpec\",\"value\":"
            + RecordUtils.toJsonString(valid)
            + "}]";
    assertEquals(
        validator
            .validateProposedAspects(
                null,
                Set.of(
                    TestPatchMCP.of(
                        SERVICE_URN, Constants.SERVICE_DEFINITION_ASPECT_NAME, validOps)),
                null)
            .count(),
        0,
        "Patch with a decodable rawSpec should pass");
  }

  @Test
  public void testCorruptGzipBlobRejected() {
    // Declares GZIP but the blob is not valid base64 -> decode throws -> one violation.
    final LargeString corrupt = new LargeString();
    corrupt.setCompression(CompressionType.GZIP);
    corrupt.setBlob("!!!not-valid-base64!!!");

    final ServiceDefinition definition = new ServiceDefinition();
    definition.setFormat(ServiceDefinitionFormat.OPENAPI);
    definition.setRawSpec(corrupt);

    final List<AspectValidationException> exceptions = validate(definition);
    assertEquals(exceptions.size(), 1);
    assertTrue(exceptions.get(0).getMessage().contains("failed to decode"));
  }
}

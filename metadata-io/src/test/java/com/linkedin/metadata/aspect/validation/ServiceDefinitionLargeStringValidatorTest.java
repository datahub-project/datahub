package com.linkedin.metadata.aspect.validation;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.CompressionType;
import com.linkedin.common.LargeString;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.utils.LargeStrings;
import com.linkedin.service.ServiceDefinition;
import com.linkedin.service.ServiceDefinitionFormat;
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

  // A patch-applied write reaches pre-commit as a merged UPSERT ChangeMCP, so the decode check
  // runs here rather than on the raw proposal.
  private ChangeMCP mcpFor(final ServiceDefinition definition) {
    final ChangeMCP item = mock(ChangeMCP.class);
    when(item.getAspect(ServiceDefinition.class)).thenReturn(definition);
    when(item.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(item.getUrn()).thenReturn(SERVICE_URN);
    when(item.getAspectName()).thenReturn(Constants.SERVICE_DEFINITION_ASPECT_NAME);
    return item;
  }

  private List<AspectValidationException> preCommit(final ServiceDefinition definition) {
    return validator
        .validatePreCommitAspects(null, Set.of(mcpFor(definition)), null)
        .collect(Collectors.toList());
  }

  private List<AspectValidationException> proposed(final ServiceDefinition definition) {
    return validator
        .validateProposedAspects(null, Set.of(mcpFor(definition)), null)
        .collect(Collectors.toList());
  }

  @Test
  public void testValidRawSpecPasses() {
    // An encoder-produced rawSpec decodes cleanly -> no violation.
    final ServiceDefinition definition = new ServiceDefinition();
    definition.setFormat(ServiceDefinitionFormat.OPENAPI);
    definition.setRawSpec(LargeStrings.encode("openapi: 3.0.0"));

    assertTrue(preCommit(definition).isEmpty());
  }

  @Test
  public void testCorruptRawSpecRejectedAtPreCommit() {
    // Declares GZIP but the blob is not valid base64 -> decode throws -> one violation. Running at
    // pre-commit means a value written via PATCH (merged into an UPSERT here) is also caught, not
    // just full UPSERTs.
    final LargeString corrupt = new LargeString();
    corrupt.setCompression(CompressionType.GZIP);
    corrupt.setBlob("!!!not-valid-base64!!!");

    final ServiceDefinition definition = new ServiceDefinition();
    definition.setFormat(ServiceDefinitionFormat.OPENAPI);
    definition.setRawSpec(corrupt);

    final List<AspectValidationException> exceptions = preCommit(definition);
    assertEquals(exceptions.size(), 1);
    assertTrue(exceptions.get(0).getMessage().contains("failed to decode"));
  }

  @Test
  public void testProposedHookReturnsEmptyForCorruptRawSpec() {
    // The check moved to pre-commit; the proposed hook must no longer reject, otherwise a PATCH
    // (dropped by the proposed-time supportedOperations gate) would bypass validation again.
    final LargeString corrupt = new LargeString();
    corrupt.setCompression(CompressionType.GZIP);
    corrupt.setBlob("!!!not-valid-base64!!!");

    final ServiceDefinition definition = new ServiceDefinition();
    definition.setFormat(ServiceDefinitionFormat.OPENAPI);
    definition.setRawSpec(corrupt);

    assertTrue(proposed(definition).isEmpty());
  }
}

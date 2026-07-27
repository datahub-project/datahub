package com.linkedin.metadata.aspect.validation;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Aliases;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class AliasesValidatorTest {
  private static final TestEntityRegistry TEST_REGISTRY = new TestEntityRegistry();
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA.TABLE,PROD)");
  private static final Urn LOWERCASED_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)");

  private final AliasesValidator validator = new AliasesValidator();
  private final RetrieverContext retrieverContext = mock(RetrieverContext.class);

  @Test
  public void testAcceptsCorrectLowercasedUrn() {
    // The value the side effect itself writes — must pass (no self-rejection).
    Aliases aspect = new Aliases().setLowercasedUrn(LOWERCASED_URN);
    List<AspectValidationException> exceptions =
        validator
            .validateProposedAspects(
                OperationFingerprint.EMPTY,
                TestMCP.ofOneUpsertItem(DATASET_URN, aspect, TEST_REGISTRY),
                retrieverContext)
            .collect(Collectors.toList());
    assertTrue(exceptions.isEmpty());
  }

  @Test
  public void testRejectsWrongLowercasedUrn() {
    // A client-supplied value that is not the canonical lowercased form is rejected.
    Aliases aspect = new Aliases().setLowercasedUrn(DATASET_URN);
    List<AspectValidationException> exceptions =
        validator
            .validateProposedAspects(
                OperationFingerprint.EMPTY,
                TestMCP.ofOneUpsertItem(DATASET_URN, aspect, TEST_REGISTRY),
                retrieverContext)
            .collect(Collectors.toList());
    assertEquals(exceptions.size(), 1);
  }
}

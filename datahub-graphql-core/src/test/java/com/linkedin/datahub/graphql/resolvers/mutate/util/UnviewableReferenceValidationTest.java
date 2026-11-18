package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.datahub.graphql.TestUtils.stubExistingUrns;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.ViewAuthorizationTestContexts;
import com.linkedin.datahub.graphql.resolvers.domain.SetDomainResolver;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import org.testng.annotations.Test;

/** A label or domain the caller can't view is rejected exactly like one that doesn't exist. */
public class UnviewableReferenceValidationTest {

  private static final Urn TAG_URN = UrnUtils.getUrn("urn:li:tag:hidden");
  private static final Urn DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:hidden");
  private static final Urn ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PROD)");

  private static OperationContext context(final Urn... hidden) {
    return ViewAuthorizationTestContexts.contextHiding(hidden).getOperationContext();
  }

  @Test
  public void testUnviewableLabelFailsLikeMissingLabel() {
    EntityService<?> hiddenService = mock(EntityService.class);
    stubExistingUrns(hiddenService, TAG_URN);
    EntityService<?> missingService = mock(EntityService.class);
    stubExistingUrns(missingService);

    IllegalArgumentException hidden =
        expectThrows(
            IllegalArgumentException.class,
            () ->
                LabelUtils.validateLabels(
                    context(TAG_URN), List.of(TAG_URN), Constants.TAG_ENTITY_NAME, hiddenService));
    IllegalArgumentException missing =
        expectThrows(
            IllegalArgumentException.class,
            () ->
                LabelUtils.validateLabels(
                    context(), List.of(TAG_URN), Constants.TAG_ENTITY_NAME, missingService));

    assertEquals(hidden.getMessage(), missing.getMessage());
    LabelUtils.validateLabels(
        context(), List.of(TAG_URN), Constants.TAG_ENTITY_NAME, hiddenService);
  }

  @Test
  public void testUnviewableDomainFailsLikeMissingDomain() {
    EntityService<?> hiddenService = mock(EntityService.class);
    stubExistingUrns(hiddenService, DOMAIN_URN);
    EntityService<?> missingService = mock(EntityService.class);
    stubExistingUrns(missingService);

    IllegalArgumentException hidden =
        expectThrows(
            IllegalArgumentException.class,
            () -> DomainUtils.validateDomain(context(DOMAIN_URN), DOMAIN_URN, hiddenService));
    IllegalArgumentException missing =
        expectThrows(
            IllegalArgumentException.class,
            () -> DomainUtils.validateDomain(context(), DOMAIN_URN, missingService));

    assertEquals(hidden.getMessage(), missing.getMessage());
    DomainUtils.validateDomain(context(), DOMAIN_URN, hiddenService);
  }

  @Test
  public void testSetDomainUnviewableDomainFailsLikeMissingDomain() {
    EntityService<?> hiddenService = mock(EntityService.class);
    when(hiddenService.exists(any(), any(Urn.class), eq(true))).thenReturn(true);
    EntityService<?> missingService = mock(EntityService.class);
    when(missingService.exists(any(), eq(DOMAIN_URN), eq(true))).thenReturn(false);
    when(missingService.exists(any(), eq(ENTITY_URN), eq(true))).thenReturn(true);

    IllegalArgumentException hidden =
        expectThrows(
            IllegalArgumentException.class,
            () ->
                SetDomainResolver.validateSetDomainInput(
                    context(DOMAIN_URN), ENTITY_URN, DOMAIN_URN, hiddenService));
    IllegalArgumentException missing =
        expectThrows(
            IllegalArgumentException.class,
            () ->
                SetDomainResolver.validateSetDomainInput(
                    context(), ENTITY_URN, DOMAIN_URN, missingService));

    assertEquals(hidden.getMessage(), missing.getMessage());
    SetDomainResolver.validateSetDomainInput(context(), ENTITY_URN, DOMAIN_URN, hiddenService);
  }
}

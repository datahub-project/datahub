package com.linkedin.metadata.entity.validation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.Deprecation;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * After a rollback, known aspects can reference entity types only a newer build registered. Those
 * references are dropped instead of stored or indexed, and the rest of the aspect is accepted.
 */
public class UnknownEntityUrnStrippingTest {

  private static final Urn DATA_PRODUCT_URN = UrnUtils.getUrn("urn:li:dataProduct:dp");
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)");
  private static final Urn UNKNOWN_TYPE_URN = UrnUtils.getUrn("urn:li:entityFromNewerBuild:x");

  private final EntityRegistry entityRegistry =
      TestOperationContexts.systemContextNoSearchAuthorization().getEntityRegistry();
  private AspectRetriever aspectRetriever;

  @BeforeClass
  public void setUp() {
    aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
  }

  @Test
  public void testArrayElementWithRequiredUnknownUrnIsRemoved() {
    // destinationUrn is required, so the whole association is dropped, not just the urn.
    DataProductProperties aspect =
        new DataProductProperties()
            .setAssets(
                new DataProductAssociationArray(
                    new DataProductAssociation().setDestinationUrn(DATASET_URN),
                    new DataProductAssociation().setDestinationUrn(UNKNOWN_TYPE_URN)));

    ValidationApiUtils.validateRecordTemplate(
        entityRegistry.getEntitySpec("dataProduct"), DATA_PRODUCT_URN, aspect, aspectRetriever);

    assertEquals(aspect.getAssets().size(), 1);
    assertEquals(aspect.getAssets().get(0).getDestinationUrn(), DATASET_URN);
  }

  @Test
  public void testUnknownUrnsInUrnArrayAreRemoved() {
    Urn domain = UrnUtils.getUrn("urn:li:domain:finance");
    Domains aspect =
        new Domains().setDomains(new UrnArray(UNKNOWN_TYPE_URN, domain, UNKNOWN_TYPE_URN));

    ValidationApiUtils.validateRecordTemplate(
        entityRegistry.getEntitySpec("dataset"), DATASET_URN, aspect, aspectRetriever);

    assertEquals(aspect.getDomains(), List.of(domain));
  }

  @Test
  public void testOptionalFieldWithUnknownUrnIsRemoved() {
    Deprecation aspect =
        new Deprecation()
            .setDeprecated(true)
            .setNote("moved")
            .setActor(UrnUtils.getUrn("urn:li:corpuser:alice"))
            .setReplacement(UNKNOWN_TYPE_URN);

    ValidationApiUtils.validateRecordTemplate(
        entityRegistry.getEntitySpec("dataset"), DATASET_URN, aspect, aspectRetriever);

    assertFalse(aspect.hasReplacement());
    assertEquals(aspect.getNote(), "moved");
  }

  @Test
  public void testRequiredTopLevelUnknownUrnStillFailsTheAspect() {
    // Nothing removable encloses a required top-level field: only this aspect is rejected.
    Deprecation aspect =
        new Deprecation().setDeprecated(true).setNote("moved").setActor(UNKNOWN_TYPE_URN);

    assertThrows(
        ValidationException.class,
        () ->
            ValidationApiUtils.validateRecordTemplate(
                entityRegistry.getEntitySpec("dataset"), DATASET_URN, aspect, aspectRetriever));
    assertEquals(aspect.getActor(), UNKNOWN_TYPE_URN);
  }

  @Test
  public void testKnownTypeOnInvalidDestinationIsStillRejected() {
    // Only unknown entity types are stripped; a known type that is not a valid destination is
    // still a validation error.
    DataProductProperties aspect =
        new DataProductProperties()
            .setAssets(
                new DataProductAssociationArray(
                    new DataProductAssociation()
                        .setDestinationUrn(UrnUtils.getUrn("urn:li:corpuser:alice"))));

    assertThrows(
        ValidationException.class,
        () ->
            ValidationApiUtils.validateRecordTemplate(
                entityRegistry.getEntitySpec("dataProduct"),
                DATA_PRODUCT_URN,
                aspect,
                aspectRetriever));
  }
}

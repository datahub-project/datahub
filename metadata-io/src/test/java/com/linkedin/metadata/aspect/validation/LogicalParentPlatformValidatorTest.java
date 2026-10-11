package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DATA_PLATFORM_INFO_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Edge;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.entity.Aspect;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LogicalParentPlatformValidatorTest {

  private static final Urn LOGICAL_PARENT_DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:logical,model,PROD)");
  private static final Urn NON_LOGICAL_PARENT_DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.parent,PROD)");
  private static final Urn CHILD_DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t,PROD)");

  private final EntityRegistry registry =
      TestOperationContexts.systemContextNoSearchAuthorization().getEntityRegistry();

  private LogicalParentPlatformValidator validator;
  private CachingAspectRetriever mockAspectRetriever;
  private RetrieverContext retrieverContext;
  private Map<Urn, Aspect> stubbedPlatforms;

  @BeforeMethod
  public void setup() {
    validator =
        new LogicalParentPlatformValidator()
            .setConfig(
                AspectPluginConfig.builder()
                    .className("test")
                    .enabled(true)
                    .supportedOperations(List.of("UPSERT"))
                    .supportedEntityAspectNames(List.of())
                    .build());
    mockAspectRetriever = Mockito.mock(CachingAspectRetriever.class);
    stubbedPlatforms = new HashMap<>();
    Mockito.when(
            mockAspectRetriever.getLatestAspectObjects(
                any(OperationFingerprint.class), any(), any()))
        .thenAnswer(
            invocation -> {
              Set<Urn> urns = invocation.getArgument(1);
              return urns.stream()
                  .filter(stubbedPlatforms::containsKey)
                  .collect(
                      Collectors.toMap(
                          urn -> urn,
                          urn ->
                              Map.of(DATA_PLATFORM_INFO_ASPECT_NAME, stubbedPlatforms.get(urn))));
            });
    retrieverContext =
        io.datahubproject.metadata.context.RetrieverContext.builder()
            .searchRetriever(Mockito.mock(SearchRetriever.class))
            .graphRetriever(Mockito.mock(GraphRetriever.class))
            .cachingAspectRetriever(mockAspectRetriever)
            .build();
  }

  private void stubPlatform(Urn platformUrn, boolean logical) {
    DataPlatformInfo info = new DataPlatformInfo().setLogical(logical);
    stubbedPlatforms.put(platformUrn, new Aspect(info.data()));
  }

  private BatchItem datasetLink(Urn parentDatasetUrn) {
    LogicalParent aspect =
        new LogicalParent().setParent(new Edge().setDestinationUrn(parentDatasetUrn));
    return TestMCP.ofOneUpsertItem(CHILD_DATASET, aspect, registry).stream().findFirst().get();
  }

  @Test
  public void rejectsNonLogicalParentPlatform() {
    stubPlatform(UrnUtils.getUrn("urn:li:dataPlatform:snowflake"), false);

    List<AspectValidationException> exceptions =
        validator
            .validateProposedAspects(
                OperationFingerprint.EMPTY,
                List.of(datasetLink(NON_LOGICAL_PARENT_DATASET)),
                retrieverContext)
            .toList();

    Assert.assertEquals(exceptions.size(), 1);
  }

  @Test
  public void allowsLogicalParentPlatform() {
    stubPlatform(UrnUtils.getUrn("urn:li:dataPlatform:logical"), true);

    List<AspectValidationException> exceptions =
        validator
            .validateProposedAspects(
                OperationFingerprint.EMPTY,
                List.of(datasetLink(LOGICAL_PARENT_DATASET)),
                retrieverContext)
            .toList();

    Assert.assertTrue(exceptions.isEmpty());
  }

  @Test
  public void allowsSchemaFieldParentOnLogicalPlatform() {
    stubPlatform(UrnUtils.getUrn("urn:li:dataPlatform:logical"), true);
    Urn childFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(CHILD_DATASET, "id");
    Urn parentFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(LOGICAL_PARENT_DATASET, "id");
    LogicalParent aspect =
        new LogicalParent().setParent(new Edge().setDestinationUrn(parentFieldUrn));
    BatchItem item =
        TestMCP.ofOneUpsertItem(childFieldUrn, aspect, registry).stream().findFirst().get();

    List<AspectValidationException> exceptions =
        validator
            .validateProposedAspects(OperationFingerprint.EMPTY, List.of(item), retrieverContext)
            .toList();

    Assert.assertTrue(exceptions.isEmpty());
  }

  @Test
  public void rejectsMalformedParentUrn() {
    // A forged MCP could set logicalParent.parent to any URN type. UrnAnnotationValidator
    // enforces the dataset/schemaField entityTypes constraint separately, but there is no
    // guaranteed ordering between validators, so this must fail cleanly rather than throw.
    Urn corpUserUrn = UrnUtils.getUrn("urn:li:corpuser:jdoe");

    List<AspectValidationException> exceptions =
        validator
            .validateProposedAspects(
                OperationFingerprint.EMPTY, List.of(datasetLink(corpUserUrn)), retrieverContext)
            .toList();

    Assert.assertEquals(exceptions.size(), 1);
  }

  @Test
  public void batchesPlatformLookupsAcrossItems() {
    stubPlatform(UrnUtils.getUrn("urn:li:dataPlatform:logical"), true);
    stubPlatform(UrnUtils.getUrn("urn:li:dataPlatform:snowflake"), false);

    List<AspectValidationException> exceptions =
        validator
            .validateProposedAspects(
                OperationFingerprint.EMPTY,
                List.of(
                    datasetLink(LOGICAL_PARENT_DATASET), datasetLink(NON_LOGICAL_PARENT_DATASET)),
                retrieverContext)
            .toList();

    Assert.assertEquals(exceptions.size(), 1);
    Mockito.verify(mockAspectRetriever, Mockito.times(1))
        .getLatestAspectObjects(any(OperationFingerprint.class), any(), any());
  }

  @Test
  public void rejectsEveryItemSharingANonLogicalPlatform() {
    stubPlatform(UrnUtils.getUrn("urn:li:dataPlatform:snowflake"), false);
    Urn otherParent =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.parent2,PROD)");

    List<AspectValidationException> exceptions =
        validator
            .validateProposedAspects(
                OperationFingerprint.EMPTY,
                List.of(datasetLink(NON_LOGICAL_PARENT_DATASET), datasetLink(otherParent)),
                retrieverContext)
            .toList();

    Assert.assertEquals(exceptions.size(), 2);
  }

  @Test
  public void ignoresUnlink() {
    LogicalParent unlink = new LogicalParent();
    BatchItem item =
        TestMCP.ofOneUpsertItem(CHILD_DATASET, unlink, registry).stream().findFirst().get();

    List<AspectValidationException> exceptions =
        validator
            .validateProposedAspects(OperationFingerprint.EMPTY, List.of(item), retrieverContext)
            .toList();

    Assert.assertTrue(exceptions.isEmpty());
    Mockito.verifyNoInteractions(mockAspectRetriever);
  }
}

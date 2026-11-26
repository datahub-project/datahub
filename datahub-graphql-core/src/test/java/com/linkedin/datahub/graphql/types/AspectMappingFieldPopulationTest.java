package com.linkedin.datahub.graphql.types;

import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.access.token.DataHubAccessTokenInfo;
import com.linkedin.chart.ChartInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.ChangeAuditStamps;
import com.linkedin.common.FabricType;
import com.linkedin.common.MLFeatureDataType;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.AccessTokenMetadata;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.DataHubView;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.MLFeature;
import com.linkedin.datahub.graphql.generated.MLFeatureTable;
import com.linkedin.datahub.graphql.generated.MLModel;
import com.linkedin.datahub.graphql.generated.MLModelGroup;
import com.linkedin.datahub.graphql.generated.MLPrimaryKey;
import com.linkedin.datahub.graphql.generated.Tag;
import com.linkedin.datahub.graphql.types.auth.mappers.AccessTokenMetadataMapper;
import com.linkedin.datahub.graphql.types.chart.mappers.ChartMapper;
import com.linkedin.datahub.graphql.types.corpuser.mappers.CorpUserMapper;
import com.linkedin.datahub.graphql.types.dataset.mappers.DatasetMapper;
import com.linkedin.datahub.graphql.types.domain.DomainMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.MLFeatureMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.MLFeatureTableMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.MLModelGroupMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.MLModelMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.MLPrimaryKeyMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagMapper;
import com.linkedin.datahub.graphql.types.view.DataHubViewMapper;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.ChartKey;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.key.DomainKey;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.ml.metadata.MLFeatureProperties;
import com.linkedin.ml.metadata.MLFeatureTableProperties;
import com.linkedin.ml.metadata.MLModelGroupProperties;
import com.linkedin.ml.metadata.MLModelProperties;
import com.linkedin.ml.metadata.MLPrimaryKeyProperties;
import com.linkedin.tag.TagProperties;
import com.linkedin.view.DataHubViewDefinition;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
import org.testng.annotations.Test;

/**
 * Proves that when the optimizer fetches ONLY the aspects a field's {@code @aspectMapping}
 * declares, the corresponding GraphQL field still populates via the real mapper. This guards
 * against a wrong or incomplete aspect list silently returning null fields (which the
 * missing-directive fallback does NOT protect against).
 */
public class AspectMappingFieldPopulationTest {

  private static EnvelopedAspect env(RecordTemplate aspect) {
    return new EnvelopedAspect().setValue(new Aspect(aspect.data()));
  }

  @Test
  public void testDatasetNameAndPropertiesPopulateFromMappedAspects() throws Exception {
    Urn urn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_table,PROD)");
    DatasetKey key =
        new DatasetKey()
            .setPlatform(Urn.createFromTuple("dataPlatform", "mysql"))
            .setName("my_db.my_table")
            .setOrigin(FabricType.PROD);
    DatasetProperties props = new DatasetProperties().setName("My Table").setDescription("desc");

    // Only the aspects mapped for `name`/`properties`: datasetKey + datasetProperties.
    EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setUrn(urn)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        Constants.DATASET_KEY_ASPECT_NAME, env(key),
                        Constants.DATASET_PROPERTIES_ASPECT_NAME, env(props))));

    Dataset dataset = DatasetMapper.map(null, response);

    assertNotNull(dataset);
    assertEquals(dataset.getUrn(), urn.toString());
    assertNotNull(dataset.getProperties(), "properties must populate from datasetProperties");
    assertEquals(dataset.getProperties().getName(), "My Table");
    assertNotNull(dataset.getName(), "name must populate from mapped aspects");
  }

  @Test
  public void testChartPropertiesPopulateFromChartInfo() throws Exception {
    Urn urn = Urn.createFromString("urn:li:chart:(looker,my_chart)");
    ChartKey key = new ChartKey().setChartId("my_chart").setDashboardTool("looker");
    AuditStamp stamp =
        new AuditStamp().setTime(0L).setActor(Urn.createFromString("urn:li:corpuser:test"));
    ChartInfo info =
        new ChartInfo()
            .setTitle("My Chart")
            .setDescription("chart desc")
            .setLastModified(new ChangeAuditStamps().setCreated(stamp).setLastModified(stamp));

    EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.CHART_ENTITY_NAME)
            .setUrn(urn)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        Constants.CHART_KEY_ASPECT_NAME, env(key),
                        Constants.CHART_INFO_ASPECT_NAME, env(info))));

    Chart chart = ChartMapper.map(null, response);

    assertNotNull(chart);
    assertEquals(chart.getUrn(), urn.toString());
    assertNotNull(chart.getProperties(), "properties must populate from chartInfo");
    assertEquals(chart.getProperties().getName(), "My Chart");
  }

  @Test
  public void testDomainPropertiesPopulateFromDomainProperties() throws Exception {
    Urn urn = Urn.createFromString("urn:li:domain:my-domain");
    DomainProperties props = new DomainProperties().setName("My Domain").setDescription("dom desc");

    EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DOMAIN_ENTITY_NAME)
            .setUrn(urn)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        Constants.DOMAIN_KEY_ASPECT_NAME, env(new DomainKey().setId("my-domain")),
                        Constants.DOMAIN_PROPERTIES_ASPECT_NAME, env(props))));

    Domain domain = DomainMapper.map(null, response);

    assertNotNull(domain);
    assertEquals(domain.getUrn(), urn.toString());
    assertNotNull(domain.getProperties(), "properties must populate from domainProperties");
    assertEquals(domain.getProperties().getName(), "My Domain");
  }

  @Test
  public void testDataHubViewFieldsPopulateFromViewInfo() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataHubView:test-view");
    Urn actor = Urn.createFromString("urn:li:corpuser:test");
    DataHubViewInfo info =
        new DataHubViewInfo()
            .setType(DataHubViewType.PERSONAL)
            .setName("view-name")
            .setDescription("view-desc")
            .setCreated(new AuditStamp().setTime(1L).setActor(actor))
            .setLastModified(new AuditStamp().setTime(1L).setActor(actor))
            .setDefinition(
                new DataHubViewDefinition()
                    .setEntityTypes(
                        new StringArray(ImmutableList.of(Constants.DATASET_ENTITY_NAME)))
                    .setFilter(
                        new Filter()
                            .setOr(
                                new ConjunctiveCriterionArray(
                                    ImmutableList.of(
                                        new ConjunctiveCriterion()
                                            .setAnd(new CriterionArray()))))));

    EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATAHUB_VIEW_ENTITY_NAME)
            .setUrn(urn)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(Constants.DATAHUB_VIEW_INFO_ASPECT_NAME, env(info))));

    DataHubView view = DataHubViewMapper.map(null, response);

    assertNotNull(view);
    assertEquals(view.getName(), "view-name");
    assertEquals(view.getDescription(), "view-desc");
    assertEquals(view.getViewType().toString(), "PERSONAL");
    assertNotNull(view.getDefinition());
  }

  /**
   * Suggested in review: a soft-deleted Document loaded under an optimized {urn, exists} selection
   * must report exists == false. The exists field maps to the status aspect and the hydration table
   * forces documentInfo + subTypes, so the mapper sees exactly these three aspects.
   */
  @Test
  public void testDocumentExistsFalseForRemovedEntityFromStatusAspect() throws Exception {
    Urn urn = Urn.createFromString("urn:li:document:removed-doc");
    Urn actor = Urn.createFromString("urn:li:corpuser:test");

    com.linkedin.knowledge.DocumentInfo info =
        new com.linkedin.knowledge.DocumentInfo()
            .setStatus(
                new com.linkedin.knowledge.DocumentStatus()
                    .setState(com.linkedin.knowledge.DocumentState.PUBLISHED))
            .setContents(new com.linkedin.knowledge.DocumentContents().setText("contents"))
            .setCreated(new AuditStamp().setTime(1L).setActor(actor))
            .setLastModified(new AuditStamp().setTime(1L).setActor(actor));
    com.linkedin.common.SubTypes subTypes =
        new com.linkedin.common.SubTypes()
            .setTypeNames(new StringArray(ImmutableList.of("Document")));
    com.linkedin.common.Status status = new com.linkedin.common.Status().setRemoved(true);

    EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DOCUMENT_ENTITY_NAME)
            .setUrn(urn)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        Constants.DOCUMENT_INFO_ASPECT_NAME, env(info),
                        Constants.SUB_TYPES_ASPECT_NAME, env(subTypes),
                        Constants.STATUS_ASPECT_NAME, env(status))));

    com.linkedin.datahub.graphql.generated.Document document =
        com.linkedin.datahub.graphql.types.knowledge.DocumentMapper.map(null, response);

    assertNotNull(document);
    assertEquals(document.getExists(), Boolean.FALSE);
  }

  @Test
  public void testTagDescriptionPopulatesFromTagProperties() throws Exception {
    Urn urn = Urn.createFromString("urn:li:tag:my-tag");
    TagProperties props =
        new TagProperties().setName("my-tag").setDescription("tag description from properties");

    EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.TAG_ENTITY_NAME)
            .setUrn(urn)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(Constants.TAG_PROPERTIES_ASPECT_NAME, env(props))));

    Tag tag = TagMapper.map(null, response);

    assertNotNull(tag);
    assertEquals(tag.getDescription(), "tag description from properties");
    assertNotNull(tag.getProperties());
    assertEquals(tag.getProperties().getDescription(), "tag description from properties");
  }

  @Test
  public void testAccessTokenMetadataFieldsPopulateFromTokenInfo() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataHubAccessToken:token-id");
    Urn actor = Urn.createFromString("urn:li:corpuser:actor");
    Urn owner = Urn.createFromString("urn:li:corpuser:owner");
    DataHubAccessTokenInfo info =
        new DataHubAccessTokenInfo()
            .setName("token-name")
            .setDescription("token-desc")
            .setActorUrn(actor)
            .setOwnerUrn(owner)
            .setCreatedAt(100L)
            .setExpiresAt(200L);

    EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.ACCESS_TOKEN_ENTITY_NAME)
            .setUrn(urn)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(Constants.ACCESS_TOKEN_INFO_NAME, env(info))));

    AccessTokenMetadata metadata = AccessTokenMetadataMapper.map(null, response);

    assertNotNull(metadata);
    assertEquals(metadata.getName(), "token-name");
    assertEquals(metadata.getDescription(), "token-desc");
    assertEquals(metadata.getActorUrn(), actor.toString());
    assertEquals(metadata.getOwnerUrn(), owner.toString());
    assertEquals(metadata.getCreatedAt(), Long.valueOf(100L));
    assertEquals(metadata.getExpiresAt(), Long.valueOf(200L));
  }

  @Test
  public void testCorpUserTagsAndGlobalTagsPopulateFromGlobalTagsAspect() throws Exception {
    Urn urn = Urn.createFromString("urn:li:corpuser:tagged-user");
    com.linkedin.common.GlobalTags tags =
        new com.linkedin.common.GlobalTags().setTags(new TagAssociationArray());
    EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.CORP_USER_ENTITY_NAME)
            .setUrn(urn)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(Constants.GLOBAL_TAGS_ASPECT_NAME, env(tags))));

    CorpUser user = CorpUserMapper.map(null, response);

    assertNotNull(user.getTags(), "tags must populate from globalTags");
    assertNotNull(user.getGlobalTags(), "deprecated globalTags alias must remain populated");
  }

  @Test
  public void testMlTopLevelFieldsPopulateFromPropertiesAspects() throws Exception {
    Urn featureUrn = Urn.createFromString("urn:li:mlFeature:(ns,feat)");
    MLFeature feature =
        MLFeatureMapper.map(
            null,
            new EntityResponse()
                .setEntityName(Constants.ML_FEATURE_ENTITY_NAME)
                .setUrn(featureUrn)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ML_FEATURE_PROPERTIES_ASPECT_NAME,
                            env(
                                new MLFeatureProperties()
                                    .setDescription("feature-desc")
                                    .setDataType(MLFeatureDataType.CONTINUOUS))))));
    assertEquals(feature.getDescription(), "feature-desc");
    assertEquals(
        feature.getDataType().toString(),
        com.linkedin.datahub.graphql.generated.MLFeatureDataType.CONTINUOUS.toString());

    Urn primaryKeyUrn = Urn.createFromString("urn:li:mlPrimaryKey:(ns,pk)");
    MLPrimaryKey primaryKey =
        MLPrimaryKeyMapper.map(
            null,
            new EntityResponse()
                .setEntityName(Constants.ML_PRIMARY_KEY_ENTITY_NAME)
                .setUrn(primaryKeyUrn)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ML_PRIMARY_KEY_PROPERTIES_ASPECT_NAME,
                            env(
                                new MLPrimaryKeyProperties()
                                    .setDescription("pk-desc")
                                    .setDataType(MLFeatureDataType.ORDINAL)
                                    .setSources(new UrnArray()))))));
    assertEquals(primaryKey.getDescription(), "pk-desc");
    assertNotNull(primaryKey.getPrimaryKeyProperties());
    assertEquals(primaryKey.getPrimaryKeyProperties().getDescription(), "pk-desc");

    Urn tableUrn = Urn.createFromString("urn:li:mlFeatureTable:(urn:li:dataPlatform:feast,tbl)");
    MLFeatureTable table =
        MLFeatureTableMapper.map(
            null,
            new EntityResponse()
                .setEntityName(Constants.ML_FEATURE_TABLE_ENTITY_NAME)
                .setUrn(tableUrn)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ML_FEATURE_TABLE_PROPERTIES_ASPECT_NAME,
                            env(new MLFeatureTableProperties().setDescription("table-desc"))))));
    assertEquals(table.getDescription(), "table-desc");
    assertNotNull(table.getFeatureTableProperties());
    assertEquals(table.getFeatureTableProperties().getDescription(), "table-desc");

    Urn modelUrn =
        Urn.createFromString("urn:li:mlModel:(urn:li:dataPlatform:sagemaker,model,PROD)");
    MLModel model =
        MLModelMapper.map(
            null,
            new EntityResponse()
                .setEntityName(Constants.ML_MODEL_ENTITY_NAME)
                .setUrn(modelUrn)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ML_MODEL_PROPERTIES_ASPECT_NAME,
                            env(new MLModelProperties().setDescription("model-desc"))))));
    assertEquals(model.getDescription(), "model-desc");

    Urn groupUrn =
        Urn.createFromString("urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,group,PROD)");
    MLModelGroup group =
        MLModelGroupMapper.map(
            null,
            new EntityResponse()
                .setEntityName(Constants.ML_MODEL_GROUP_ENTITY_NAME)
                .setUrn(groupUrn)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ML_MODEL_GROUP_PROPERTIES_ASPECT_NAME,
                            env(new MLModelGroupProperties().setDescription("group-desc"))))));
    assertEquals(group.getDescription(), "group-desc");
  }
}

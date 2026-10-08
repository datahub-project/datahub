package com.linkedin.datahub.graphql.types.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Origin;
import com.linkedin.common.OriginType;
import com.linkedin.common.SourceDetails;
import com.linkedin.common.SourceDetailsArray;
import com.linkedin.common.SyncMechanism;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.generated.DataHubPolicy;
import com.linkedin.datahub.graphql.generated.DataHubSubscription;
import com.linkedin.datahub.graphql.generated.EntityChangeType;
import com.linkedin.datahub.graphql.generated.PolicyState;
import com.linkedin.datahub.graphql.generated.PolicyType;
import com.linkedin.datahub.graphql.generated.SubscriptionType;
import com.linkedin.datahub.graphql.types.common.mappers.OriginMapper;
import com.linkedin.datahub.graphql.types.policy.DataHubPolicyMapper;
import com.linkedin.datahub.graphql.types.schemafield.SchemaFieldMapper;
import com.linkedin.datahub.graphql.types.subscription.mappers.DataHubSubscriptionMapper;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.subscription.SubscriptionInfo;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Data a newer version wrote (read after a rollback) can hold enum values and references to entity
 * types this version doesn't know. Mappers fall back or leave those items out instead of failing
 * the entity, list or batch.
 */
public class RollbackToleranceMappersTest {

  private static final String NEWER_VALUE = "VALUE_FROM_NEWER_BUILD";
  private static final Urn DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)");
  private static final Urn PLATFORM = UrnUtils.getUrn("urn:li:dataPlatform:hive");
  private static final Urn FROM_NEWER_BUILD = UrnUtils.getUrn("urn:li:entityFromNewerBuild:x");
  private static final AuditStamp STAMP =
      new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn("urn:li:corpuser:u"));

  @Test
  public void testOriginSourceDetailsGraphQLCantRepresentAreLeftOut() {
    SourceDetails known =
        new SourceDetails()
            .setSource(DATASET)
            .setPlatform(PLATFORM)
            .setLastModified(STAMP)
            .setMechanism(SyncMechanism.INGEST);
    known.data().put("mechanism", NEWER_VALUE);
    SourceDetails unknownSource =
        new SourceDetails()
            .setSource(FROM_NEWER_BUILD)
            .setPlatform(PLATFORM)
            .setLastModified(STAMP)
            .setMechanism(SyncMechanism.INGEST);
    Origin origin =
        new Origin()
            .setType(OriginType.NATIVE)
            .setSourceDetails(new SourceDetailsArray(unknownSource, known));

    com.linkedin.datahub.graphql.generated.Origin mapped =
        OriginMapper.map(TestUtils.getMockAllowContext(), origin);

    assertEquals(mapped.getRawSourceDetails().size(), 1);
    assertEquals(mapped.getResolvedSourceDetails().getSource().getUrn(), DATASET.toString());
    assertEquals(
        mapped.getResolvedSourceDetails().getMechanism(),
        com.linkedin.datahub.graphql.generated.SyncMechanism.OTHER);
  }

  @Test
  public void testOriginWithOnlyUnrepresentableSourceDetailsHasNone() {
    Origin origin =
        new Origin()
            .setType(OriginType.NATIVE)
            .setSourceDetails(
                new SourceDetailsArray(
                    new SourceDetails()
                        .setSource(FROM_NEWER_BUILD)
                        .setPlatform(PLATFORM)
                        .setLastModified(STAMP)
                        .setMechanism(SyncMechanism.API)));

    com.linkedin.datahub.graphql.generated.Origin mapped =
        OriginMapper.map(TestUtils.getMockAllowContext(), origin);

    assertNull(mapped.getResolvedSourceDetails());
    assertNull(mapped.getRawSourceDetails());
  }

  @Test
  public void testPolicyTypeAndStateFallBack() {
    DataHubPolicy unknown = mapPolicy(NEWER_VALUE, NEWER_VALUE);
    assertEquals(unknown.getPolicyType(), PolicyType.METADATA);
    assertEquals(unknown.getState(), PolicyState.INACTIVE);

    DataHubPolicy known = mapPolicy("PLATFORM", "ACTIVE");
    assertEquals(known.getPolicyType(), PolicyType.PLATFORM);
    assertEquals(known.getState(), PolicyState.ACTIVE);
  }

  @Test
  public void testSubscriptionSkipsUnknownTypesAndChangeTypes() {
    SubscriptionInfo info = subscriptionInfo(DATASET);
    info.data().put("types", new DataList(List.of("ENTITY_CHANGE", NEWER_VALUE)));
    info.data()
        .put(
            "entityChangeTypes",
            new DataList(
                List.of(
                    new DataMap(Map.of("entityChangeType", "OPERATION_COLUMN_ADDED")),
                    new DataMap(Map.of("entityChangeType", NEWER_VALUE)))));

    DataHubSubscription mapped =
        DataHubSubscriptionMapper.map(
            TestUtils.getMockAllowContext(),
            Map.entry(UrnUtils.getUrn("urn:li:subscription:s"), info));

    assertEquals(mapped.getSubscriptionTypes(), List.of(SubscriptionType.ENTITY_CHANGE));
    assertEquals(mapped.getEntityChangeTypes().size(), 1);
    assertEquals(
        mapped.getEntityChangeTypes().get(0).getEntityChangeType(),
        EntityChangeType.OPERATION_COLUMN_ADDED);
    assertNotNull(mapped.getEntity());
  }

  @Test
  public void testSchemaFieldOfUnrepresentableParentResolvesToNothing() {
    EntityResponse response =
        new EntityResponse()
            .setUrn(UrnUtils.getUrn("urn:li:schemaField:(" + FROM_NEWER_BUILD + ",field)"))
            .setAspects(new EnvelopedAspectMap());

    assertNull(SchemaFieldMapper.map(TestUtils.getMockAllowContext(), response));
  }

  @Test
  public void testSchemaFieldOfKnownParentIsMapped() {
    EntityResponse response =
        new EntityResponse()
            .setUrn(UrnUtils.getUrn("urn:li:schemaField:(" + DATASET + ",field)"))
            .setAspects(new EnvelopedAspectMap());

    assertEquals(
        SchemaFieldMapper.map(TestUtils.getMockAllowContext(), response).getFieldPath(), "field");
  }

  private static DataHubPolicy mapPolicy(String type, String state) {
    DataHubPolicyInfo info =
        new DataHubPolicyInfo()
            .setDisplayName("p")
            .setDescription("d")
            .setType("METADATA")
            .setState("ACTIVE")
            .setPrivileges(new StringArray())
            .setActors(new DataHubActorFilter())
            .setEditable(true);
    info.data().put("type", type);
    info.data().put("state", state);
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.DATAHUB_POLICY_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(info.data())));
    return DataHubPolicyMapper.map(
        null,
        new EntityResponse().setUrn(UrnUtils.getUrn("urn:li:dataHubPolicy:p")).setAspects(aspects));
  }

  private static SubscriptionInfo subscriptionInfo(Urn entity) {
    return new SubscriptionInfo()
        .setActorUrn(UrnUtils.getUrn("urn:li:corpuser:u"))
        .setActorType("corpuser")
        .setTypes(new com.linkedin.subscription.SubscriptionTypeArray())
        .setCreatedOn(STAMP)
        .setUpdatedOn(STAMP)
        .setEntityUrn(entity);
  }
}

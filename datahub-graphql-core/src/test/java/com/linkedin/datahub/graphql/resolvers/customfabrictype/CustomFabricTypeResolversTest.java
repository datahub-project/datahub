package com.linkedin.datahub.graphql.resolvers.customfabrictype;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContextWithOperationContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.Deprecation;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CustomFabricType;
import com.linkedin.datahub.graphql.generated.ListCustomFabricTypesResult;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.fabrictype.CustomFabricTypeInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

/** A1, plus the list filters, for the custom environment resolvers. */
public class CustomFabricTypeResolversTest {

  private static final Urn MY_ENV = UrnUtils.getUrn("urn:li:customFabricType:MY_ENV");
  private static final Urn OLD_ENV = UrnUtils.getUrn("urn:li:customFabricType:OLD_ENV");
  private static final Urn GONE_ENV = UrnUtils.getUrn("urn:li:customFabricType:GONE_ENV");
  private static final Urn ACTOR = UrnUtils.getUrn("urn:li:corpuser:admin");

  @Test
  public void createWhenAuthorized() throws Exception {
    EntityClient client = mock(EntityClient.class);

    CustomFabricType result =
        new CreateCustomFabricTypeResolver(client).get(createEnv(getMockAllowContext())).join();

    assertEquals(result.getUrn(), MY_ENV.toString());
    assertEquals(result.getId(), "MY_ENV");
    assertEquals(result.getProperties().getDisplayName(), "My env");
    verify(client).ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());
  }

  @Test
  public void createDeniedWithoutPrivilege() throws Exception {
    EntityClient client = mock(EntityClient.class);

    assertThrows(
        CompletionException.class,
        () ->
            new CreateCustomFabricTypeResolver(client)
                .get(createEnv(getMockDenyContextWithOperationContext()))
                .join());
    verify(client, never()).ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());
  }

  @Test
  public void createExistingIsAConflict() throws Exception {
    EntityClient client = mock(EntityClient.class);
    when(client.exists(any(), eq(MY_ENV))).thenReturn(true);

    assertThrows(
        CompletionException.class,
        () ->
            new CreateCustomFabricTypeResolver(client)
                .get(createEnv(getMockAllowContext()))
                .join());
    verify(client, never()).ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());
  }

  @Test
  public void updateKeepsFieldsLeftNull() throws Exception {
    EntityClient client = mock(EntityClient.class);
    when(client.getV2(any(), eq(Constants.CUSTOM_FABRIC_TYPE_ENTITY_NAME), eq(MY_ENV), anySet()))
        .thenReturn(
            response(
                MY_ENV, new CustomFabricTypeInfo().setDisplayName("Old").setDescription("Kept")));
    QueryContext context = getMockAllowContext();
    DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    when(env.getContext()).thenReturn(context);
    when(env.getArgument("input"))
        .thenReturn(Map.of("urn", MY_ENV.toString(), "displayName", "New"));

    CustomFabricType result = new UpdateCustomFabricTypeResolver(client).get(env).join();

    assertEquals(result.getProperties().getDisplayName(), "New");
    assertEquals(result.getProperties().getDescription(), "Kept");
    verify(client).ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());
  }

  @Test
  public void updateDeniedWithoutPrivilege() throws Exception {
    EntityClient client = mock(EntityClient.class);
    DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    QueryContext denied = getMockDenyContextWithOperationContext();
    when(env.getContext()).thenReturn(denied);
    when(env.getArgument("input")).thenReturn(Map.of("urn", MY_ENV.toString()));

    assertThrows(
        CompletionException.class,
        () -> new UpdateCustomFabricTypeResolver(client).get(env).join());
    verify(client, never()).ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());
  }

  @Test
  public void deleteRequiresPrivilege() throws Exception {
    EntityClient client = mock(EntityClient.class);
    assertTrue(
        new DeleteCustomFabricTypeResolver(client).get(deleteEnv(getMockAllowContext())).join());
    verify(client).deleteEntity(any(), eq(MY_ENV));

    EntityClient untouched = mock(EntityClient.class);
    assertThrows(
        CompletionException.class,
        () ->
            new DeleteCustomFabricTypeResolver(untouched)
                .get(deleteEnv(getMockDenyContextWithOperationContext()))
                .join());
    verify(untouched, never()).deleteEntity(any(), any());
  }

  /** Update and delete act only on environments, whatever the caller is allowed to manage. */
  @Test
  public void updateAndDeleteRefuseOtherEntityTypes() throws Exception {
    Urn dataset = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.tbl,PROD)");
    EntityClient client = mock(EntityClient.class);
    QueryContext allowed = getMockAllowContext();
    DataFetchingEnvironment deleteEnv = mock(DataFetchingEnvironment.class);
    when(deleteEnv.getContext()).thenReturn(allowed);
    when(deleteEnv.getArgument("urn")).thenReturn(dataset.toString());

    assertThrows(
        CompletionException.class,
        () -> new DeleteCustomFabricTypeResolver(client).get(deleteEnv).join());
    verify(client, never()).deleteEntity(any(), any());

    DataFetchingEnvironment updateEnv = mock(DataFetchingEnvironment.class);
    when(updateEnv.getContext()).thenReturn(allowed);
    when(updateEnv.getArgument("input"))
        .thenReturn(Map.of("urn", dataset.toString(), "displayName", "x"));

    assertThrows(
        CompletionException.class,
        () -> new UpdateCustomFabricTypeResolver(client).get(updateEnv).join());
    verify(client, never()).ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());
  }

  @Test
  public void listLeavesOutSoftDeletedAndOptionallyDeprecated() throws Exception {
    EntityClient client = mock(EntityClient.class);
    when(client.listUrns(any(), eq(Constants.CUSTOM_FABRIC_TYPE_ENTITY_NAME), eq(0), anyInt()))
        .thenReturn(
            new ListUrnsResult()
                .setEntities(new UrnArray(List.of(OLD_ENV, MY_ENV, GONE_ENV)))
                .setStart(0)
                .setCount(3)
                .setTotal(3));
    Map<Urn, EntityResponse> responses = new HashMap<>();
    responses.put(MY_ENV, response(MY_ENV));
    responses.put(
        OLD_ENV,
        response(
            OLD_ENV, new Deprecation().setDeprecated(true).setNote("Retired.").setActor(ACTOR)));
    responses.put(GONE_ENV, response(GONE_ENV, new Status().setRemoved(true)));
    when(client.batchGetV2(any(), eq(Constants.CUSTOM_FABRIC_TYPE_ENTITY_NAME), anySet(), anySet()))
        .thenReturn(responses);

    assertEquals(ids(list(client, Map.of())), List.of("MY_ENV", "OLD_ENV"));
    assertEquals(ids(list(client, Map.of("includeDeprecated", false))), List.of("MY_ENV"));
    ListCustomFabricTypesResult paged = list(client, Map.of("start", 1, "count", 1));
    assertEquals(ids(paged), List.of("OLD_ENV"));
    assertEquals(paged.getTotal(), 2);
  }

  private static ListCustomFabricTypesResult list(EntityClient client, Map<String, Object> input)
      throws Exception {
    QueryContext context = getMockAllowContext();
    DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    when(env.getContext()).thenReturn(context);
    when(env.getArgument("input")).thenReturn(input);
    return new ListCustomFabricTypesResolver(client).get(env).join();
  }

  private static List<String> ids(ListCustomFabricTypesResult result) {
    return result.getCustomFabricTypes().stream()
        .map(CustomFabricType::getId)
        .collect(Collectors.toList());
  }

  private static EntityResponse response(Urn urn, RecordTemplate... aspects) {
    EnvelopedAspectMap map = new EnvelopedAspectMap();
    for (RecordTemplate aspect : aspects) {
      String name =
          aspect instanceof CustomFabricTypeInfo
              ? Constants.CUSTOM_FABRIC_TYPE_INFO_ASPECT_NAME
              : aspect instanceof Deprecation
                  ? Constants.DEPRECATION_ASPECT_NAME
                  : Constants.STATUS_ASPECT_NAME;
      map.put(name, new EnvelopedAspect().setValue(new Aspect(aspect.data())));
    }
    return new EntityResponse()
        .setUrn(urn)
        .setEntityName(Constants.CUSTOM_FABRIC_TYPE_ENTITY_NAME)
        .setAspects(map);
  }

  private static DataFetchingEnvironment createEnv(QueryContext context) {
    DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    when(env.getContext()).thenReturn(context);
    when(env.getArgument("input")).thenReturn(Map.of("id", "MY_ENV", "displayName", "My env"));
    return env;
  }

  private static DataFetchingEnvironment deleteEnv(QueryContext context) {
    DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    when(env.getContext()).thenReturn(context);
    when(env.getArgument("urn")).thenReturn(MY_ENV.toString());
    return env;
  }
}

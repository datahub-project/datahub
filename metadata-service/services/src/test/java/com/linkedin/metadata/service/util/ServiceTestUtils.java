package com.linkedin.metadata.service.util;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.ViewProperties;
import com.linkedin.domain.Domains;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.query.QuerySubjects;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import io.datahubproject.openapi.v2.models.BatchGetUrnRequest;
import io.datahubproject.openapi.v2.models.BatchGetUrnResponse;
import io.datahubproject.openapi.v2.models.GenericEntity;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.mockito.Mockito;

public class ServiceTestUtils {
  private ServiceTestUtils() {}

  public static final Urn TEST_ENTITY_URN_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test,PROD)");
  public static final Urn TEST_ENTITY_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test1,PROD)");

  private static final Map<String, Class<? extends RecordTemplate>> aspectSpecMap =
      Map.of(
          Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME, EditableSchemaMetadata.class,
          Constants.GLOSSARY_TERMS_ASPECT_NAME, GlossaryTerms.class,
          Constants.GLOBAL_TAGS_ASPECT_NAME, GlobalTags.class,
          Constants.DOMAINS_ASPECT_NAME, Domains.class,
          Constants.OWNERSHIP_ASPECT_NAME, Ownership.class,
          Constants.VIEW_PROPERTIES_ASPECT_NAME, ViewProperties.class,
          Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME, GlobalSettingsInfo.class,
          Constants.QUERY_SUBJECTS_ASPECT_NAME, QuerySubjects.class,
          Constants.CORP_USER_SETTINGS_ASPECT_NAME, CorpUserSettings.class,
          Constants.STATUS_ASPECT_NAME, Status.class);

  public static OpenApiClient createMockGlobalTagsClient(@Nullable GlobalTags existingGlobalTags)
      throws Exception {
    return createMockClient(existingGlobalTags, Constants.GLOBAL_TAGS_ASPECT_NAME);
  }

  public static OpenApiClient createMockDomainsClient(@Nullable Domains domains) throws Exception {
    return createMockClient(domains, Constants.DOMAINS_ASPECT_NAME);
  }

  public static OpenApiClient createMockSchemaMetadataClient(
      @Nullable EditableSchemaMetadata existingMetadata) throws Exception {
    return createMockClient(existingMetadata, Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
  }

  public static OpenApiClient createMockOwnersClient(@Nullable Ownership existingOwnership)
      throws Exception {
    return createMockClient(existingOwnership, Constants.OWNERSHIP_ASPECT_NAME);
  }

  public static OpenApiClient createMockGlossaryClient(
      @Nullable GlossaryTerms existingGlossaryTerms) throws Exception {
    return createMockClient(existingGlossaryTerms, Constants.GLOSSARY_TERMS_ASPECT_NAME);
  }

  private static OpenApiClient createMockClient(@Nullable RecordTemplate aspect, String aspectName)
      throws Exception {
    OpenApiClient mockClient =
        Mockito.mock(OpenApiClient.class, Mockito.withSettings().verboseLogging());
    BatchGetUrnRequest batchGetUrnRequest =
        new BatchGetUrnRequest(
            List.of(TEST_ENTITY_URN_1.toString(), TEST_ENTITY_URN_2.toString()),
            Collections.singletonList(aspectName),
            true);

    List<GenericEntity> entities;
    if (aspect != null) {
      Map<String, Pair<RecordTemplate, SystemMetadata>> aspectMap1 = new HashMap<>();
      aspectMap1.put(aspectName, Pair.of(aspect, null));
      GenericEntity testEntity1 =
          GenericEntity.builder()
              .urn(TEST_ENTITY_URN_1.toString())
              .build(new ObjectMapper(), aspectMap1);

      Map<String, Pair<RecordTemplate, SystemMetadata>> aspectMap2 = new HashMap<>();
      aspectMap2.put(aspectName, Pair.of(aspect, null));
      GenericEntity testEntity2 =
          GenericEntity.builder()
              .urn(TEST_ENTITY_URN_2.toString())
              .build(new ObjectMapper(), aspectMap2);
      entities = new ArrayList<>();
      entities.add(testEntity1);
      entities.add(testEntity2);
    } else {
      entities = Collections.emptyList();
    }

    BatchGetUrnResponse batchGetUrnResponse = new BatchGetUrnResponse(entities);
    Mockito.when(
            mockClient.getBatchUrns(
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(batchGetUrnRequest),
                Mockito.nullable(String.class)))
        .thenReturn(batchGetUrnResponse);
    OperationContext mockContext = Mockito.mock(OperationContext.class);
    Mockito.when(mockClient.getSystemOperationContext()).thenReturn(mockContext);
    EntityRegistry mockRegistry = Mockito.mock(EntityRegistry.class);
    Mockito.when(mockContext.getEntityRegistry()).thenReturn(mockRegistry);
    EntitySpec mockEntitySpec = Mockito.mock(EntitySpec.class);
    Mockito.when(mockRegistry.getEntitySpec(Mockito.eq(Constants.DATASET_ENTITY_NAME)))
        .thenReturn(mockEntitySpec);
    AspectSpec mockAspectSpec = Mockito.mock(AspectSpec.class);
    Mockito.when(mockEntitySpec.getAspectSpec(Mockito.anyString())).thenReturn(mockAspectSpec);
    Mockito.when(mockAspectSpec.getDataTemplateClass())
        .thenReturn((Class<RecordTemplate>) aspectSpecMap.get(aspectName));
    return mockClient;
  }

  public static Authentication mockAuthentication() {
    Authentication mockAuth = Mockito.mock(Authentication.class);
    Mockito.when(mockAuth.getActor()).thenReturn(new Actor(ActorType.USER, Constants.SYSTEM_ACTOR));
    return mockAuth;
  }
}

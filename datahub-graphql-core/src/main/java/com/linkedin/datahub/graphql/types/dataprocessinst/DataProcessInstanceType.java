package com.linkedin.datahub.graphql.types.dataprocessinst;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.DataProcessInstance;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.dataprocessinst.mappers.DataProcessInstanceMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DataProcessInstanceType
    implements com.linkedin.datahub.graphql.types.EntityType<DataProcessInstance, String> {

  public static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(
          DATA_PROCESS_INSTANCE_KEY_ASPECT_NAME,
          DATA_PLATFORM_INSTANCE_ASPECT_NAME,
          DATA_PROCESS_INSTANCE_PROPERTIES_ASPECT_NAME,
          DATA_PROCESS_INSTANCE_INPUT_ASPECT_NAME,
          DATA_PROCESS_INSTANCE_OUTPUT_ASPECT_NAME,
          DATA_PROCESS_INSTANCE_RUN_EVENT_ASPECT_NAME,
          TEST_RESULTS_ASPECT_NAME,
          DATA_PROCESS_INSTANCE_RELATIONSHIPS_ASPECT_NAME,
          ML_TRAINING_RUN_PROPERTIES_ASPECT_NAME,
          SUB_TYPES_ASPECT_NAME,
          CONTAINER_ASPECT_NAME);

  private final EntityClient _entityClient;
  private final FeatureFlags _featureFlags;

  @Override
  public EntityType type() {
    return EntityType.DATA_PROCESS_INSTANCE;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<DataProcessInstance> objectClass() {
    return DataProcessInstance.class;
  }

  @Override
  public List<DataFetcherResult<DataProcessInstance>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> dataProcessInstanceUrns =
        urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    try {
      Map<Urn, EntityResponse> entities = new HashMap<>();
      if (_featureFlags.isDataProcessInstanceEntityEnabled()) {
        entities =
            _entityClient.batchGetV2(
                context.getOperationContext(),
                DATA_PROCESS_INSTANCE_ENTITY_NAME,
                new HashSet<>(dataProcessInstanceUrns),
                ASPECTS_TO_FETCH);
      }

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : dataProcessInstanceUrns) {
        if (_featureFlags.isDataProcessInstanceEntityEnabled()) {
          gmsResults.add(entities.getOrDefault(urn, null));
        }
      }

      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<DataProcessInstance>newResult()
                          .data(DataProcessInstanceMapper.map(context, gmsResult))
                          .build())
          .collect(Collectors.toList());

    } catch (Exception e) {
      throw new RuntimeException("Failed to load Data Process Instance entity", e);
    }
  }
}

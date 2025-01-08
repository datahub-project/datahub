package com.linkedin.datahub.graphql.types.dataprocessinst;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataProcessInstance;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.dataprocessinst.mappers.DataProcessInstanceMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class DataProcessInstanceType
    implements com.linkedin.datahub.graphql.types.EntityType<DataProcessInstance, String> {

  private static final Set<String> ASPECTS_TO_RESOLVE =
      ImmutableSet.of(
          DATA_PROCESS_INSTANCE_KEY_ASPECT_NAME,
          DATA_PROCESS_INSTANCE_INPUT_ASPECT_NAME,
          DATA_PROCESS_INSTANCE_OUTPUT_ASPECT_NAME,
          DATA_PROCESS_INSTANCE_RELATIONSHIPS_ASPECT_NAME,
          STATUS_ASPECT_NAME,
          TEST_RESULTS_ASPECT_NAME,
          LINEAGE_FEATURES_ASPECT_NAME);

  private static final Set<String> FACET_FIELDS = ImmutableSet.of("origin", "platform");
  private static final String ENTITY_NAME = "dataProcessInstance";

  private final EntityClient entityClient;

  public DataProcessInstanceType(final EntityClient entityClient) {
    this.entityClient = entityClient;
  }

  @Override
  public Class<DataProcessInstance> objectClass() {
    return DataProcessInstance.class;
  }

  @Override
  public EntityType type() {
    return EntityType.DATA_PROCESS_INSTANCE;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public List<DataFetcherResult<DataProcessInstance>> batchLoad(
      @Nonnull final List<String> urnStrs, @Nonnull final QueryContext context) {
    try {
      final List<Urn> urns = urnStrs.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

      final Map<Urn, EntityResponse> dataProcessInstanceMap =
          entityClient.batchGetV2(
              context.getOperationContext(),
              DATA_PROCESS_INSTANCE_ENTITY_NAME,
              urns.stream()
                  .filter(urn -> canView(context.getOperationContext(), urn))
                  .collect(Collectors.toSet()),
              ASPECTS_TO_RESOLVE);

      final List<EntityResponse> gmsResults = new ArrayList<>(urnStrs.size());
      for (Urn urn : urns) {
        gmsResults.add(dataProcessInstanceMap.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsDataProcessInstance ->
                  gmsDataProcessInstance == null
                      ? null
                      : DataFetcherResult.<DataProcessInstance>newResult()
                          .data(DataProcessInstanceMapper.map(context, gmsDataProcessInstance))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Datasets", e);
    }
  }
}

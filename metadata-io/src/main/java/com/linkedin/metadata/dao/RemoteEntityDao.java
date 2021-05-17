package com.linkedin.metadata.dao;

import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.DynamicRecordMetadata;
import com.linkedin.data.template.FieldDef;
import com.linkedin.experimental.Entity;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.ActionRequestBuilder;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.RestliRequestOptions;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.ResourceSpec;
import com.linkedin.restli.common.ResourceSpecImpl;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import javax.annotation.Nonnull;


/**
 * Dao that fetches entities from the Entity Resource
 */
public class RemoteEntityDao {

  private static final String ACTION_INGEST = "ingest";
  private static final String PARAM_ENTITY = "entity";
  private static final String RESOURCE_NAME = "entities";

  protected final Client _restliClient;

  public RemoteEntityDao(@Nonnull final Client restliClient) {
    _restliClient = restliClient;
  }

  public void create(@Nonnull final Entity entity) throws IllegalArgumentException, RemoteInvocationException {

    // Replace with EntitiesDoIngestActionBuilder.

    final FieldDef<?> entityFieldDef = new FieldDef<>(PARAM_ENTITY, Entity.class, DataTemplateUtil.getSchema(String.class));

    final HashMap<String, DynamicRecordMetadata> actionRequestMetadata = new HashMap<>();
    actionRequestMetadata.put(ACTION_INGEST, new DynamicRecordMetadata(ACTION_INGEST, Arrays.asList(entityFieldDef)));

    final HashMap<java.lang.String, DynamicRecordMetadata> actionResponseMetadata = new HashMap<>();
    actionResponseMetadata.put(ACTION_INGEST, new DynamicRecordMetadata(ACTION_INGEST, Collections.emptyList()));

    final ResourceSpec resourceSpec =
        new ResourceSpecImpl(Collections.emptySet(), actionRequestMetadata, actionResponseMetadata, String.class,
            EmptyRecord.class, EmptyRecord.class, EmptyRecord.class, Collections.emptyMap());

    final ActionRequestBuilder builder =
        new ActionRequestBuilder(RESOURCE_NAME, Void.class, resourceSpec, RestliRequestOptions.DEFAULT_OPTIONS);

    builder.name(ACTION_INGEST);
    builder.addParam(entityFieldDef, entity);

    final Request request = builder.build();

    try {
      _restliClient.sendRequest(request).getResponse();
    } catch (RemoteInvocationException e) {
      throw new RemoteInvocationException(e);
    }
  }
}

package com.linkedin.metadata.dao;

import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.DynamicRecordMetadata;
import com.linkedin.data.template.FieldDef;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.experimental.Entity;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.ActionRequestBuilder;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.RestliRequestOptions;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.ResourceSpec;
import com.linkedin.restli.common.ResourceSpecImpl;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

public class EntityRemoteWriterDao {

    protected final Client _restliClient;

    public EntityRemoteWriterDao(@Nonnull Client restliClient) {
        _restliClient = restliClient;
    }

    public void create(@Nonnull RecordTemplate entity)
            throws IllegalArgumentException, RemoteInvocationException {

        // TODO: John Refactor this

        final FieldDef<?> entityFieldDef = new FieldDef<>("entity", Entity.class, DataTemplateUtil.getSchema(String.class));
        final HashMap<String, DynamicRecordMetadata> actionRequestMetadata = new HashMap<>();

        actionRequestMetadata.put("ingest", new DynamicRecordMetadata("ingest", Arrays.asList(entityFieldDef)));

        final HashMap<java.lang.String, DynamicRecordMetadata> actionResponseMetadata = new HashMap<>();
        actionResponseMetadata.put("ingest", new DynamicRecordMetadata("ingest", Collections.emptyList()));

        final ResourceSpec resourceSpec = new ResourceSpecImpl(
                Collections.emptySet(),
                actionRequestMetadata,
                actionResponseMetadata,
                String.class,
                EmptyRecord.class,
                EmptyRecord.class,
                EmptyRecord.class,
                Collections.emptyMap());

        final ActionRequestBuilder builder =
                new ActionRequestBuilder(
                        "entities",
                        Void.class,
                        resourceSpec,
                        RestliRequestOptions.DEFAULT_OPTIONS);

        builder.name("ingest");
        builder.addParam(entityFieldDef, entity);

        final Request request = builder.build();

        try {
            _restliClient.sendRequest(request).getResponse();
        } catch (RemoteInvocationException e) {
            throw new RemoteInvocationException(e);
        }
    }
}

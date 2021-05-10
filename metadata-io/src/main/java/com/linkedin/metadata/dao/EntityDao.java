package com.linkedin.metadata.dao;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.NamedDataSchema;
import com.linkedin.data.template.DataTemplate;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.kafka.EntityEventProducer;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.ebean.config.ServerConfig;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class EntityDao {

    private final Map<String, BaseLocalDAO> _entityNameToLocalDao;

    public EntityDao(@Nonnull final EntityRegistry registry,
                     @Nonnull final EntityEventProducer producer,
                     @Nonnull final ServerConfig serverConfig) {
        _entityNameToLocalDao = new HashMap<>();
        final List<EntitySpec> entitySpecs = registry.getEntitySpecs();
        for (final EntitySpec spec : entitySpecs) {
            // Create a new BaseMetadataEventProducer for Kafka
            _entityNameToLocalDao.put(
                    spec.getName(),
                    new EbeanLocalDAO(
                            getDataSchemaClassFromSchema(spec.getAspectTyperefSchema()),
                            producer.getProducer(spec.getName()),
                            serverConfig,
                            Urn.class // Note that the Ebean local dao actually does want a specific urn here.
                    )
            );
        }
    }

    public Map<AspectKey<Urn, ? extends RecordTemplate>, Optional<? extends RecordTemplate>> get(
            @Nonnull String entityName,
            @Nonnull Set<AspectKey<Urn, ? extends RecordTemplate>> keys) {
        return _entityNameToLocalDao.get(entityName).get(keys);
    }

    private Class<? extends DataTemplate> getDataSchemaClassFromSchema(final NamedDataSchema schema) {
        Class<? extends DataTemplate> clazz;
        try {
            clazz = Class.forName(schema.getFullName()).asSubclass(DataTemplate.class);
        } catch (ClassNotFoundException e) {
            throw new ModelConversionException("Unable to find class " + schema.getFullName(), e);
        }
        return clazz;
    }
}

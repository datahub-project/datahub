package com.linkedin.metadata.event;

import com.linkedin.data.schema.NamedDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.dao.producer.BaseMetadataEventProducer;
import com.linkedin.metadata.dao.producer.KafkaMetadataEventProducer;
import com.linkedin.metadata.dao.producer.KafkaProducerCallback;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import org.apache.avro.generic.IndexedRecord;
import com.linkedin.mxe.TopicConvention;
import org.apache.kafka.clients.producer.Producer;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EntityEventProducer {

    private final Map<String, BaseMetadataEventProducer> _entityNameToProducer;

    public EntityEventProducer(@Nonnull final EntityRegistry registry,
                               @Nonnull final Producer<String, ? extends IndexedRecord> producer,
                               @Nonnull final TopicConvention topicConvention) {
        _entityNameToProducer = new HashMap<>();
        final List<EntitySpec> entitySpecs = registry.getEntitySpecs();
        for (final EntitySpec spec : entitySpecs) {
            // Create a new BaseMetadataEventProducer for Kafka
            _entityNameToProducer.put(
                    spec.getName(),
                    new KafkaMetadataEventProducer(
                            getRecordTemplateClassFromSchema(spec.getSnapshotSchema()),
                            getRecordTemplateClassFromSchema(spec.getAspectTyperefSchema()),
                            producer,
                            topicConvention,
                            new KafkaProducerCallback()
                    )
            );
        }
    }

    public BaseMetadataEventProducer getProducer(@Nonnull final String entityName) {
        return _entityNameToProducer.get(entityName);
    }

    private Class<? extends RecordTemplate> getRecordTemplateClassFromSchema(final NamedDataSchema schema) {
        Class<? extends RecordTemplate> clazz;
        try {
            clazz = Class.forName(schema.getFullName()).asSubclass(RecordTemplate.class);
        } catch (ClassNotFoundException e) {
            throw new ModelConversionException("Unable to find class " + schema.getFullName(), e);
        }
        return clazz;
    }
}

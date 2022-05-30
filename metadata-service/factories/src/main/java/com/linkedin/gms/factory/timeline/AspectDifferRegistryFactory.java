package com.linkedin.gms.factory.timeline;

import com.linkedin.metadata.timeline.differ.AspectDifferRegistry;
import com.linkedin.metadata.timeline.differ.DatasetPropertiesDiffer;
import com.linkedin.metadata.timeline.differ.DeprecationDiffer;
import com.linkedin.metadata.timeline.differ.EditableDatasetPropertiesDiffer;
import com.linkedin.metadata.timeline.differ.EditableSchemaMetadataDiffer;
import com.linkedin.metadata.timeline.differ.EntityKeyDiffer;
import com.linkedin.metadata.timeline.differ.GlobalTagsDiffer;
import com.linkedin.metadata.timeline.differ.GlossaryTermsDiffer;
import com.linkedin.metadata.timeline.differ.InstitutionalMemoryDiffer;
import com.linkedin.metadata.timeline.differ.OwnershipDiffer;
import com.linkedin.metadata.timeline.differ.SchemaMetadataDiffer;
import com.linkedin.metadata.timeline.differ.SingleDomainDiffer;
import com.linkedin.metadata.timeline.differ.StatusDiffer;
import javax.annotation.Nonnull;
import javax.inject.Singleton;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.linkedin.metadata.Constants.*;


@Configuration
public class AspectDifferRegistryFactory {

  @Bean(name = "aspectDifferRegistry")
  @Singleton
  @Nonnull
  protected AspectDifferRegistry aspectDifferRegistry() {
    final AspectDifferRegistry registry = new AspectDifferRegistry();
    registry.register(SCHEMA_METADATA_ASPECT_NAME, new SchemaMetadataDiffer());
    registry.register(EDITABLE_SCHEMA_METADATA_ASPECT_NAME, new EditableSchemaMetadataDiffer());
    registry.register(GLOBAL_TAGS_ASPECT_NAME, new GlobalTagsDiffer());
    registry.register(GLOSSARY_TERMS_ASPECT_NAME, new GlossaryTermsDiffer());
    registry.register(OWNERSHIP_ASPECT_NAME, new OwnershipDiffer());
    registry.register(INSTITUTIONAL_MEMORY_ASPECT_NAME, new InstitutionalMemoryDiffer());
    registry.register(DATASET_PROPERTIES_ASPECT_NAME, new DatasetPropertiesDiffer());
    registry.register(DOMAINS_ASPECT_NAME, new SingleDomainDiffer());
    registry.register(DATASET_PROPERTIES_ASPECT_NAME, new DatasetPropertiesDiffer());
    registry.register(EDITABLE_DATASET_PROPERTIES_ASPECT_NAME, new EditableDatasetPropertiesDiffer());

    // Entity Lifecycle Differs
    registry.register(DATASET_KEY_ASPECT_NAME, new EntityKeyDiffer<>());
    registry.register(CONTAINER_KEY_ASPECT_NAME, new EntityKeyDiffer<>());
    registry.register(CHART_KEY_ASPECT_NAME, new EntityKeyDiffer<>());
    registry.register(DASHBOARD_KEY_ASPECT_NAME, new EntityKeyDiffer<>());
    registry.register(DATA_FLOW_KEY_ASPECT_NAME, new EntityKeyDiffer<>());
    registry.register(DATA_JOB_KEY_ASPECT_NAME, new EntityKeyDiffer<>());
    registry.register(DOMAIN_KEY_ASPECT_NAME, new EntityKeyDiffer<>());
    registry.register(TAG_KEY_ASPECT_NAME, new EntityKeyDiffer<>());
    registry.register(GLOSSARY_TERM_KEY_ASPECT_NAME, new EntityKeyDiffer<>());
    registry.register(CORP_GROUP_KEY_ASPECT_NAME, new EntityKeyDiffer<>());
    registry.register(STATUS_ASPECT_NAME, new StatusDiffer());
    registry.register(DEPRECATION_ASPECT_NAME, new DeprecationDiffer());

    // TODO: Add ML models.

    return registry;
  }
}

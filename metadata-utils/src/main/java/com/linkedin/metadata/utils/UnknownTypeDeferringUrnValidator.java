package com.linkedin.metadata.utils;

import static com.linkedin.metadata.utils.UnknownDataHandlings.handling;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.NamedDataSchema;
import com.linkedin.data.schema.validator.ValidatorContext;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;

/**
 * Validates the urns of an aspect being written, except urns of entity types the registry doesn't
 * know: whether those were already stored decides, when the write is applied ({@link
 * UnknownReferenceMerger} keeps stored ones and the write rejects any other). Every other urn is
 * validated as {@link EntityRegistryUrnValidator} does.
 */
public class UnknownTypeDeferringUrnValidator extends EntityRegistryUrnValidator {

  private final EntityRegistry entityRegistry;

  public UnknownTypeDeferringUrnValidator(@Nonnull final EntityRegistry entityRegistry) {
    super(entityRegistry);
    this.entityRegistry = entityRegistry;
  }

  @Override
  protected void validateUrnField(final ValidatorContext context) {
    final DataSchema schema = context.dataElement().getSchema();
    if (DataSchema.Type.TYPEREF.equals(schema.getType())
        && ((NamedDataSchema) schema).getName().endsWith("Urn")
        && context.dataElement().getValue() instanceof String urn
        && referencesUnknownEntityType(urn)) {
      return;
    }
    super.validateUrnField(context);
  }

  private boolean referencesUnknownEntityType(@Nonnull final String urn) {
    try {
      return handling().referencesUnknownEntityType(entityRegistry, Urn.createFromString(urn));
    } catch (URISyntaxException e) {
      // Malformed urns are rejected by the validation below.
      return false;
    }
  }
}

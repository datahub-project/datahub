package io.datahubproject.test.search;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.DefaultEntitySpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EntitySpecBuilder;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.metadata.models.registry.EmptyEntityRegistry;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A one-entity registry whose aspect declares a V3 {@code TEXT_PARTIAL} subfield, so V3 mappings
 * built from it contain a {@code search_as_you_type} field. No model in the default registry
 * declares one today, which hides engine-specific partial-ngram mapping differences.
 */
public class PartialNgramEntityRegistry extends EmptyEntityRegistry {

  public static final String ENTITY_NAME = "partialNgramTestEntity";

  private static final String KEY_SCHEMA =
      "{\"type\":\"record\",\"name\":\"PartialNgramTestKey\",\"namespace\":\"com.datahub.test\","
          + "\"Aspect\":{\"name\":\"partialNgramTestKey\"},"
          + "\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";

  private static final String INFO_SCHEMA =
      "{\"type\":\"record\",\"name\":\"PartialNgramTestInfo\",\"namespace\":\"com.datahub.test\","
          + "\"Aspect\":{\"name\":\"partialNgramTestInfo\"},"
          + "\"fields\":[{\"name\":\"title\",\"type\":\"string\","
          + "\"Searchable\":{\"fieldType\":\"TEXT\",\"v3\":{\"searchLabel\":\"partialNgramTitle\","
          + "\"mappings\":[{\"type\":\"KEYWORD\"},{\"subfield\":\"ngram\",\"type\":\"TEXT_PARTIAL\"}]"
          + "}}}]}";

  private final EntitySpec entitySpec;

  public PartialNgramEntityRegistry() {
    EntitySpecBuilder builder = new EntitySpecBuilder();
    AspectSpec key = builder.buildAspectSpec(DataTemplateUtil.parseSchema(KEY_SCHEMA), null);
    RecordDataSchema infoSchema = (RecordDataSchema) DataTemplateUtil.parseSchema(INFO_SCHEMA);
    AspectSpec info = builder.buildAspectSpec(infoSchema, null);
    this.entitySpec =
        new DefaultEntitySpec(
            List.of(key, info), new EntityAnnotation(ENTITY_NAME, key.getName()), infoSchema);
  }

  @Nullable
  @Override
  public EntitySpec getEntitySpec(@Nonnull String entityName) {
    return ENTITY_NAME.equals(entityName) ? entitySpec : null;
  }

  @Nonnull
  @Override
  public Map<String, EntitySpec> getEntitySpecs() {
    return Map.of(ENTITY_NAME, entitySpec);
  }

  @Nonnull
  @Override
  public Map<String, AspectSpec> getAspectSpecs() {
    return entitySpec.getAspectSpecs().stream()
        .collect(Collectors.toMap(AspectSpec::getName, Function.identity()));
  }
}

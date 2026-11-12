package io.datahubproject.test.search;

import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.DefaultEntitySpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
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
 *
 * <p>The specs are assembled directly rather than through {@code EntitySpecBuilder}: its schema
 * annotation processing uses shared handlers, and running an ad-hoc schema through them before the
 * default registry is built breaks that later build when assertions are enabled.
 */
public class PartialNgramEntityRegistry extends EmptyEntityRegistry {

  public static final String ENTITY_NAME = "partialNgramTestEntity";
  public static final String SEARCH_LABEL = "partialNgramTitle";

  private static final String KEY_ASPECT = "partialNgramTestKey";
  private static final String INFO_ASPECT = "partialNgramTestInfo";

  private final EntitySpec entitySpec;

  public PartialNgramEntityRegistry() {
    RecordDataSchema keySchema =
        recordSchema("PartialNgramTestKey", "{\"name\":\"id\",\"type\":\"string\"}");
    RecordDataSchema infoSchema =
        recordSchema("PartialNgramTestInfo", "{\"name\":\"title\",\"type\":\"string\"}");

    DataMap ngram = new DataMap();
    ngram.put("subfield", "ngram");
    ngram.put("type", "TEXT_PARTIAL");
    DataMap v3 = new DataMap();
    v3.put("searchLabel", SEARCH_LABEL);
    v3.put("mappings", new DataList(List.of(new DataMap(Map.of("type", "KEYWORD")), ngram)));
    DataMap searchable = new DataMap();
    searchable.put("fieldType", "TEXT");
    searchable.put("v3", v3);
    SearchableFieldSpec title =
        new SearchableFieldSpec(
            new PathSpec("title"),
            SearchableAnnotation.fromPegasusAnnotationObject(
                searchable, "title", DataSchema.Type.STRING, INFO_ASPECT),
            infoSchema.getField("title").getType());

    AspectSpec key = aspectSpec(KEY_ASPECT, List.of(), keySchema);
    AspectSpec info = aspectSpec(INFO_ASPECT, List.of(title), infoSchema);
    this.entitySpec =
        new DefaultEntitySpec(
            List.of(key, info), new EntityAnnotation(ENTITY_NAME, KEY_ASPECT), infoSchema);
  }

  private static RecordDataSchema recordSchema(String name, String field) {
    return (RecordDataSchema)
        DataTemplateUtil.parseSchema(
            "{\"type\":\"record\",\"name\":\""
                + name
                + "\",\"namespace\":\"com.datahub.test\",\"fields\":["
                + field
                + "]}");
  }

  private static AspectSpec aspectSpec(
      String name, List<SearchableFieldSpec> searchable, RecordDataSchema schema) {
    return new AspectSpec(
        AspectAnnotation.fromSchemaProperty(new DataMap(Map.of("name", name)), name),
        searchable,
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        schema,
        null);
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

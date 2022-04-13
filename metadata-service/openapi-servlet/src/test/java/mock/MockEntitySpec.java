package mock;

import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.SubTypes;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.dataset.ViewProperties;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.schema.SchemaMetadata;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.linkedin.metadata.Constants.*;


public class MockEntitySpec implements EntitySpec {

  private String _name;

  public MockEntitySpec(String name) {
    _name = name;
  }

  @Override
  public String getName() {
    return _name;
  }

  @Override
  public EntityAnnotation getEntityAnnotation() {
    return null;
  }

  @Override
  public String getKeyAspectName() {
    return null;
  }

  @Override
  public AspectSpec getKeyAspectSpec() {
    DatasetKey datasetKey = new DatasetKey();
    return createAspectSpec(datasetKey, DATASET_KEY_ASPECT_NAME);
  }

  private <T extends RecordTemplate> AspectSpec createAspectSpec(T type, String name) {
    return new MockAspectSpec(new AspectAnnotation(name, false, false, null),
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), type.schema(),
        (Class<RecordTemplate>) type.getClass().asSubclass(RecordTemplate.class));
  }

  @Override
  public List<AspectSpec> getAspectSpecs() {
    return null;
  }

  @Override
  public Map<String, AspectSpec> getAspectSpecMap() {
    return Collections.emptyMap();
  }

  @Override
  public Boolean hasAspect(String name) {
    return null;
  }

  private static Map<String, RecordTemplate> ASPECT_TYPE_MAP;

  static {
    ASPECT_TYPE_MAP = new HashMap<>();
    ASPECT_TYPE_MAP.put(DATASET_KEY_ASPECT_NAME, new DatasetKey());
    ASPECT_TYPE_MAP.put(VIEW_PROPERTIES_ASPECT_NAME, new ViewProperties());
    ASPECT_TYPE_MAP.put(SCHEMA_METADATA_ASPECT_NAME, new SchemaMetadata());
    ASPECT_TYPE_MAP.put(SUB_TYPES_ASPECT_NAME, new SubTypes());
    ASPECT_TYPE_MAP.put("datasetProfile", new DatasetProfile());
    ASPECT_TYPE_MAP.put(GLOSSARY_TERMS_ASPECT_NAME, new GlossaryTerms());
  }
  @Override
  public AspectSpec getAspectSpec(String name) {
    return createAspectSpec(ASPECT_TYPE_MAP.get(name), name);
  }

  @Override
  public RecordDataSchema getSnapshotSchema() {
    return null;
  }

  @Override
  public TyperefDataSchema getAspectTyperefSchema() {
    return null;
  }
}

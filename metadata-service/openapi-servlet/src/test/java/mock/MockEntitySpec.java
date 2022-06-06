package mock;

import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.SubTypes;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.dataset.ViewProperties;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.metadata.key.DataPlatformKey;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.key.GlossaryTermKey;
import com.linkedin.metadata.key.TagKey;
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
    if (DATASET_ENTITY_NAME.equals(_name)) {
      DatasetKey datasetKey = new DatasetKey();
      return createAspectSpec(datasetKey, DATASET_KEY_ASPECT_NAME);
    } else if (DATA_PLATFORM_ENTITY_NAME.equals(_name)) {
      DataPlatformKey dataPlatformKey = new DataPlatformKey();
      return createAspectSpec(dataPlatformKey, DATA_PLATFORM_KEY_ASPECT_NAME);
    } else if (TAG_ENTITY_NAME.equals(_name)) {
      TagKey tagKey = new TagKey();
      return createAspectSpec(tagKey, TAG_KEY_ASPECT_NAME);
    } else if (GLOSSARY_TERM_ENTITY_NAME.equals(_name)) {
      GlossaryTermKey glossaryTermKey = new GlossaryTermKey();
      return createAspectSpec(glossaryTermKey, GLOSSARY_TERM_KEY_ASPECT_NAME);
    } else if (CORP_USER_ENTITY_NAME.equals(_name)) {
      CorpUserKey corpUserKey = new CorpUserKey();
      return createAspectSpec(corpUserKey, CORP_USER_KEY_ASPECT_NAME);
    }
    return null;
  }

  private <T extends RecordTemplate> AspectSpec createAspectSpec(T type, String name) {
    return new MockAspectSpec(new AspectAnnotation(name, false, false, null),
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList(), type.schema(), (Class<RecordTemplate>) type.getClass().asSubclass(RecordTemplate.class));
  }

  @Override
  public List<AspectSpec> getAspectSpecs() {
    return Collections.emptyList();
  }

  @Override
  public Map<String, AspectSpec> getAspectSpecMap() {
    return Collections.emptyMap();
  }

  @Override
  public Boolean hasAspect(String name) {
    return false;
  }

  private static final Map<String, RecordTemplate> ASPECT_TYPE_MAP;

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

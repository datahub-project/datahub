package com.linkedin.metadata.models;

import com.linkedin.data.DataMap;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.maven.artifact.versioning.ComparableVersion;

@EqualsAndHashCode
public class AspectSpec {

  private final AspectAnnotation _aspectAnnotation;
  private final Map<String, SearchableFieldSpec> _searchableFieldSpecs;
  private final Map<String, SearchScoreFieldSpec> _searchScoreFieldSpecs;
  private final Map<String, RelationshipFieldSpec> _relationshipFieldSpecs;
  private final Map<String, TimeseriesFieldSpec> _timeseriesFieldSpecs;
  private final Map<String, TimeseriesFieldCollectionSpec> _timeseriesFieldCollectionSpecs;

  // Classpath & Pegasus-specific: Temporary.
  private final RecordDataSchema _schema;
  private final Class<RecordTemplate> _aspectClass;
  @Setter @Getter private String registryName = "unknownRegistry";
  @Setter @Getter private ComparableVersion registryVersion = new ComparableVersion("0.0.0.0-dev");

  public AspectSpec(
      @Nonnull final AspectAnnotation aspectAnnotation,
      @Nonnull final List<SearchableFieldSpec> searchableFieldSpecs,
      @Nonnull final List<SearchScoreFieldSpec> searchScoreFieldSpecs,
      @Nonnull final List<RelationshipFieldSpec> relationshipFieldSpecs,
      @Nonnull final List<TimeseriesFieldSpec> timeseriesFieldSpecs,
      @Nonnull final List<TimeseriesFieldCollectionSpec> timeseriesFieldCollectionSpecs,
      final RecordDataSchema schema,
      final Class<RecordTemplate> aspectClass) {
    _aspectAnnotation = aspectAnnotation;
    _searchableFieldSpecs =
        searchableFieldSpecs.stream()
            .collect(
                Collectors.toMap(
                    spec -> spec.getPath().toString(), spec -> spec, (val1, val2) -> val1));
    _searchScoreFieldSpecs =
        searchScoreFieldSpecs.stream()
            .collect(
                Collectors.toMap(
                    spec -> spec.getPath().toString(), spec -> spec, (val1, val2) -> val1));
    _relationshipFieldSpecs =
        relationshipFieldSpecs.stream()
            .collect(
                Collectors.toMap(
                    spec -> spec.getPath().toString(), spec -> spec, (val1, val2) -> val1));
    _timeseriesFieldSpecs =
        timeseriesFieldSpecs.stream()
            .collect(
                Collectors.toMap(
                    spec -> spec.getTimeseriesFieldAnnotation().getStatName(),
                    spec -> spec,
                    (val1, val2) -> val1));
    _timeseriesFieldCollectionSpecs =
        timeseriesFieldCollectionSpecs.stream()
            .collect(
                Collectors.toMap(
                    spec -> spec.getTimeseriesFieldCollectionAnnotation().getCollectionName(),
                    spec -> spec,
                    (val1, val2) -> val1));
    _schema = schema;
    _aspectClass = aspectClass;
  }

  public String getName() {
    return _aspectAnnotation.getName();
  }

  public boolean isTimeseries() {
    return _aspectAnnotation.isTimeseries();
  }

  public Boolean isAutoRender() {
    return _aspectAnnotation.isAutoRender();
  }

  public DataMap getRenderSpec() {
    return _aspectAnnotation.getRenderSpec();
  }

  public Map<String, SearchableFieldSpec> getSearchableFieldSpecMap() {
    return _searchableFieldSpecs;
  }

  public Map<String, SearchScoreFieldSpec> getSearchScoreFieldSpecMap() {
    return _searchScoreFieldSpecs;
  }

  public Map<String, RelationshipFieldSpec> getRelationshipFieldSpecMap() {
    return _relationshipFieldSpecs;
  }

  public Map<String, TimeseriesFieldSpec> getTimeseriesFieldSpecMap() {
    return _timeseriesFieldSpecs;
  }

  public Map<String, TimeseriesFieldCollectionSpec> getTimeseriesFieldCollectionSpecMap() {
    return _timeseriesFieldCollectionSpecs;
  }

  public List<SearchableFieldSpec> getSearchableFieldSpecs() {
    return new ArrayList<>(_searchableFieldSpecs.values());
  }

  public List<SearchScoreFieldSpec> getSearchScoreFieldSpecs() {
    return new ArrayList<>(_searchScoreFieldSpecs.values());
  }

  public List<RelationshipFieldSpec> getRelationshipFieldSpecs() {
    return new ArrayList<>(_relationshipFieldSpecs.values());
  }

  public List<TimeseriesFieldSpec> getTimeseriesFieldSpecs() {
    return new ArrayList<>(_timeseriesFieldSpecs.values());
  }

  public List<TimeseriesFieldCollectionSpec> getTimeseriesFieldCollectionSpecs() {
    return new ArrayList<>(_timeseriesFieldCollectionSpecs.values());
  }

  public RecordDataSchema getPegasusSchema() {
    return _schema;
  }

  public Class<RecordTemplate> getDataTemplateClass() {
    return _aspectClass;
  }
}

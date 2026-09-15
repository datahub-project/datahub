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
  private final Map<String, SearchableRefFieldSpec> _searchableRefFieldSpecs;
  private final Map<String, UrnValidationFieldSpec> _urnValidationFieldSpecs;

  // Classpath & Pegasus-specific: Temporary.
  private final RecordDataSchema _schema;
  private final Class<RecordTemplate> _aspectClass;
  @Setter @Getter private String registryName = "unknownRegistry";
  @Setter @Getter private ComparableVersion registryVersion = new ComparableVersion("0.0.0.0-dev");

  @EqualsAndHashCode.Exclude private volatile List<SearchableFieldSpec> _searchableFieldSpecsList;

  @EqualsAndHashCode.Exclude
  private volatile List<SearchableRefFieldSpec> _searchableRefFieldSpecsList;

  @EqualsAndHashCode.Exclude private volatile List<SearchScoreFieldSpec> _searchScoreFieldSpecsList;

  @EqualsAndHashCode.Exclude
  private volatile List<RelationshipFieldSpec> _relationshipFieldSpecsList;

  @EqualsAndHashCode.Exclude private volatile List<TimeseriesFieldSpec> _timeseriesFieldSpecsList;

  @EqualsAndHashCode.Exclude
  private volatile List<TimeseriesFieldCollectionSpec> _timeseriesFieldCollectionSpecsList;

  public AspectSpec(
      @Nonnull final AspectAnnotation aspectAnnotation,
      @Nonnull final List<SearchableFieldSpec> searchableFieldSpecs,
      @Nonnull final List<SearchScoreFieldSpec> searchScoreFieldSpecs,
      @Nonnull final List<RelationshipFieldSpec> relationshipFieldSpecs,
      @Nonnull final List<TimeseriesFieldSpec> timeseriesFieldSpecs,
      @Nonnull final List<TimeseriesFieldCollectionSpec> timeseriesFieldCollectionSpecs,
      @Nonnull final List<SearchableRefFieldSpec> searchableRefFieldSpecs,
      @Nonnull final List<UrnValidationFieldSpec> urnValidationFieldSpecs,
      final RecordDataSchema schema,
      final Class<RecordTemplate> aspectClass) {
    _aspectAnnotation = aspectAnnotation;
    _searchableFieldSpecs =
        searchableFieldSpecs.stream()
            .collect(
                Collectors.toMap(
                    spec -> spec.getPath().toString(), spec -> spec, (val1, val2) -> val1));
    _searchableRefFieldSpecs =
        searchableRefFieldSpecs.stream()
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
    _urnValidationFieldSpecs =
        urnValidationFieldSpecs.stream()
            .collect(
                Collectors.toMap(
                    spec -> spec.getPath().toString(), spec -> spec, (val1, val2) -> val1));
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

  public long getSchemaVersion() {
    return _aspectAnnotation.getSchemaVersion();
  }

  @Nonnull
  public AspectAnnotation getAspectAnnotation() {
    return _aspectAnnotation;
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

  public Map<String, UrnValidationFieldSpec> getUrnValidationFieldSpecMap() {
    return _urnValidationFieldSpecs;
  }

  public Map<String, TimeseriesFieldCollectionSpec> getTimeseriesFieldCollectionSpecMap() {
    return _timeseriesFieldCollectionSpecs;
  }

  /**
   * Returns a memoized list view of the searchable field specs, backed by the same List instance on
   * every call. Callers must treat the returned List as immutable and must not mutate it in place;
   */
  public List<SearchableFieldSpec> getSearchableFieldSpecs() {
    List<SearchableFieldSpec> list = _searchableFieldSpecsList;
    if (list == null) {
      list = new ArrayList<>(_searchableFieldSpecs.values());
      _searchableFieldSpecsList = list;
    }
    return list;
  }

  /**
   * Returns a memoized list view of the searchable ref field specs, backed by the same List
   * instance on every call. Callers must treat the returned List as immutable and must not mutate
   * it in place;
   */
  public List<SearchableRefFieldSpec> getSearchableRefFieldSpecs() {
    List<SearchableRefFieldSpec> list = _searchableRefFieldSpecsList;
    if (list == null) {
      list = new ArrayList<>(_searchableRefFieldSpecs.values());
      _searchableRefFieldSpecsList = list;
    }
    return list;
  }

  /**
   * Returns a memoized list view of the search score field specs, backed by the same List instance
   * on every call. Callers must treat the returned List as immutable and must not mutate it in
   * place;
   */
  public List<SearchScoreFieldSpec> getSearchScoreFieldSpecs() {
    List<SearchScoreFieldSpec> list = _searchScoreFieldSpecsList;
    if (list == null) {
      list = new ArrayList<>(_searchScoreFieldSpecs.values());
      _searchScoreFieldSpecsList = list;
    }
    return list;
  }

  /**
   * Returns a memoized list view of the relationship field specs, backed by the same List instance
   * on every call. Callers must treat the returned List as immutable and must not mutate it in
   * place;
   */
  public List<RelationshipFieldSpec> getRelationshipFieldSpecs() {
    List<RelationshipFieldSpec> list = _relationshipFieldSpecsList;
    if (list == null) {
      list = new ArrayList<>(_relationshipFieldSpecs.values());
      _relationshipFieldSpecsList = list;
    }
    return list;
  }

  /**
   * Returns a memoized list view of the timeseries field specs, backed by the same List instance on
   * every call. Callers must treat the returned List as immutable and must not mutate it in place;
   */
  public List<TimeseriesFieldSpec> getTimeseriesFieldSpecs() {
    List<TimeseriesFieldSpec> list = _timeseriesFieldSpecsList;
    if (list == null) {
      list = new ArrayList<>(_timeseriesFieldSpecs.values());
      _timeseriesFieldSpecsList = list;
    }
    return list;
  }

  /**
   * Returns a memoized list view of the timeseries field collection specs, backed by the same List
   * instance on every call. Callers must treat the returned List as immutable and must not mutate
   * it in place;
   */
  public List<TimeseriesFieldCollectionSpec> getTimeseriesFieldCollectionSpecs() {
    List<TimeseriesFieldCollectionSpec> list = _timeseriesFieldCollectionSpecsList;
    if (list == null) {
      list = new ArrayList<>(_timeseriesFieldCollectionSpecs.values());
      _timeseriesFieldCollectionSpecsList = list;
    }
    return list;
  }

  public RecordDataSchema getPegasusSchema() {
    return _schema;
  }

  public Class<RecordTemplate> getDataTemplateClass() {
    return _aspectClass;
  }
}

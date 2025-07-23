package io.datahubproject.openapi.v1.models.registry;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.linkedin.metadata.models.AspectSpec;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AspectSpecDto {
  private AspectAnnotationDto aspectAnnotation;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private Map<String, FieldSpecDto> searchableFieldSpec;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private Map<String, FieldSpecDto> searchableRefFieldSpec;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private Map<String, FieldSpecDto> searchScoreFieldSpec;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private Map<String, FieldSpecDto> relationshipFieldSpec;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private Map<String, FieldSpecDto> timeseriesFieldSpec;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private Map<String, FieldSpecDto> timeseriesFieldCollectionSpec;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private Map<String, FieldSpecDto> urnValidationFieldSpec;

  private String registryName;
  private String registryVersion;

  public static AspectSpecDto fromAspectSpec(AspectSpec aspectSpec) {
    // Get the aspect annotation - need to use reflection since it's private
    AspectAnnotationDto aspectAnnotationDto = null;
    try {
      // The AspectSpec class has getName(), isTimeseries(), isAutoRender(), getRenderSpec() methods
      // that delegate to the AspectAnnotation
      aspectAnnotationDto =
          AspectAnnotationDto.builder()
              .name(aspectSpec.getName())
              .timeseries(aspectSpec.isTimeseries())
              .autoRender(aspectSpec.isAutoRender())
              .renderSpec(convertDataMap(aspectSpec.getRenderSpec()))
              .build();
    } catch (Exception e) {
      log.error("Error extracting aspect annotation: {}", e.getMessage());
    }

    return AspectSpecDto.builder()
        .aspectAnnotation(aspectAnnotationDto)

        // Convert Maps using the generic approach
        .searchableFieldSpec(convertFieldSpecMap(aspectSpec.getSearchableFieldSpecMap()))
        .searchScoreFieldSpec(convertFieldSpecMap(aspectSpec.getSearchScoreFieldSpecMap()))
        .relationshipFieldSpec(convertFieldSpecMap(aspectSpec.getRelationshipFieldSpecMap()))
        .timeseriesFieldSpec(convertFieldSpecMap(aspectSpec.getTimeseriesFieldSpecMap()))
        .timeseriesFieldCollectionSpec(
            convertFieldSpecMap(aspectSpec.getTimeseriesFieldCollectionSpecMap()))
        .searchableRefFieldSpec(
            convertFieldSpecMapFromList(aspectSpec.getSearchableRefFieldSpecs()))
        .urnValidationFieldSpec(convertFieldSpecMap(aspectSpec.getUrnValidationFieldSpecMap()))
        .registryName(aspectSpec.getRegistryName())
        .registryVersion(aspectSpec.getRegistryVersion().toString())
        .build();
  }

  private static <T extends com.linkedin.metadata.models.FieldSpec>
      Map<String, FieldSpecDto> convertFieldSpecMap(Map<String, T> fieldSpecMap) {
    if (fieldSpecMap == null) {
      return new HashMap<>();
    }
    Map<String, FieldSpecDto> result = new HashMap<>();
    fieldSpecMap.forEach(
        (key, value) -> {
          FieldSpecDto converted = FieldSpecDto.fromFieldSpec(value);
          if (converted != null) {
            result.put(key, converted);
          }
        });
    return result;
  }

  // Helper for searchableRefFieldSpecs which only has a List getter
  private static Map<String, FieldSpecDto> convertFieldSpecMapFromList(
      List<? extends com.linkedin.metadata.models.FieldSpec> fieldSpecs) {
    if (fieldSpecs == null || fieldSpecs.isEmpty()) {
      return null; // Return null instead of empty map
    }
    Map<String, FieldSpecDto> result = new HashMap<>();
    for (com.linkedin.metadata.models.FieldSpec spec : fieldSpecs) {
      FieldSpecDto converted = FieldSpecDto.fromFieldSpec(spec);
      if (converted != null) {
        result.put(spec.getPath().toString(), converted);
      }
    }
    return result.isEmpty() ? null : result; // Return null if result is empty
  }

  private static Map<String, Object> convertDataMap(com.linkedin.data.DataMap dataMap) {
    if (dataMap == null) {
      return null;
    }
    Map<String, Object> result = new HashMap<>();
    dataMap.forEach(result::put);
    return result;
  }
}

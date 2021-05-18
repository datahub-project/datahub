package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.annotation.SchemaAnnotationHandler;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;


public class AspectSchemaAnnotationHandler implements SchemaAnnotationHandler  {

  private final String _annotationName;
  private final PropertyOverrideComparator _comparator = new PropertyOverrideComparator();

  public AspectSchemaAnnotationHandler(final String annotationName) {
    _annotationName = annotationName;
  }

  @Override
  public ResolutionResult resolve(
      final List<Pair<String, Object>> propertiesOverrides,
      final ResolutionMetaData resolutionMetadata) {
    final ResolutionResult result = new ResolutionResult();
    final Map<String, Object> resultMap = new HashMap<>();
    final DataSchema dataSchema = resolutionMetadata.getDataSchemaUnderResolution();
    if (dataSchema != null) {
      resultMap.putAll(dataSchema.getResolvedProperties());
    }
    if (propertiesOverrides.size() == 0) {
      result.setResolvedResult(resultMap);
      return result;
    }

    propertiesOverrides.sort(_comparator);

    resultMap.put(_annotationName, propertiesOverrides.get(0).getValue());
    result.setResolvedResult(resultMap);
    return result;
  }


  @Override
  public String getAnnotationNamespace() {
    return _annotationName;
  }

  @Override
  public AnnotationValidationResult validate(
      final Map<String, Object> resolvedProperties,
      final ValidationMetaData metaData) {
    AnnotationValidationResult annotationValidationResult = new AnnotationValidationResult();
    annotationValidationResult.setValid(true);
    return annotationValidationResult;
  }
}

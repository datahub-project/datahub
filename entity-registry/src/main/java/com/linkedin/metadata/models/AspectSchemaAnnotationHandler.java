package com.linkedin.metadata.models;

import com.linkedin.data.DataComplex;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.annotation.SchemaAnnotationHandler;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;


public class AspectSchemaAnnotationHandler implements SchemaAnnotationHandler  {
  private String _annotationName;
  private PropertyOverrideComparator _comparator = new PropertyOverrideComparator();

  public AspectSchemaAnnotationHandler(final String annotationName) {
    _annotationName = annotationName;
  }

  @Override
  public ResolutionResult resolve(List<Pair<String, Object>> propertiesOverrides,
      ResolutionMetaData resolutionMetadata) {
    ResolutionResult result = new ResolutionResult();
    Map<String, Object> resultMap = new HashMap<>();
    DataSchema dataSchema = resolutionMetadata.getDataSchemaUnderResolution();
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
  public AnnotationValidationResult validate(Map<String, Object> resolvedProperties, ValidationMetaData metaData) {
    AnnotationValidationResult annotationValidationResult = new AnnotationValidationResult();
    annotationValidationResult.setValid(true);
    return annotationValidationResult;
  }
}

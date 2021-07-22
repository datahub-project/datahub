package com.linkedin.metadata.extractor;

import com.linkedin.data.DataMap;
import com.linkedin.data.element.DataElement;
import com.linkedin.data.it.IterationOrder;
import com.linkedin.data.it.ObjectIterator;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.models.FieldSpec;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Extracts fields from a RecordTemplate based on the appropriate {@link FieldSpec}.
 */
public class AspectExtractor {
  public static final String ASPECT_FIELD = "aspects";

  private AspectExtractor() {
  }

  /**
   * Function to extract the fields that match the input fieldSpecs
   */
  public static Map<String, DataElement> extractAspects(RecordTemplate snapshot) {

    final ObjectIterator iterator = new ObjectIterator(snapshot.data(), snapshot.schema(), IterationOrder.PRE_ORDER);
    final Map<String, DataElement> aspectsByName = new HashMap<>();

    for (DataElement dataElement = iterator.next(); dataElement != null; dataElement = iterator.next()) {
      final PathSpec pathSpec = dataElement.getSchemaPathSpec();
      List<String> pathComponents = pathSpec.getPathComponents();
      // three components representing /aspect/*/<aspectClassName>
      if (pathComponents.size() != 3) {
        continue;
      }
      String aspectName = PegasusUtils.getAspectNameFromFullyQualifiedName(pathComponents.get(2));
      aspectsByName.put(aspectName, dataElement);
    }

    return aspectsByName;
  }

  public static Map<String, RecordTemplate> extractAspectRecords(RecordTemplate snapshot) {
    RecordDataSchema.Field aspectField = snapshot.schema().getField(ASPECT_FIELD);
    if (aspectField == null || aspectField.getType().getType() != DataSchema.Type.ARRAY) {
      throw new IllegalArgumentException("Cannot extract aspects from a non-snapshot object (requires aspects field)");
    }
    return snapshot.data()
        .getDataList(ASPECT_FIELD)
        .stream()
        .map(unionMember -> extractAspectFromUnion((DataMap) unionMember))
        .collect(
            Collectors.toMap(record -> PegasusUtils.getAspectNameFromSchema(record.schema()), Function.identity()));
  }

  public static RecordTemplate extractAspectFromUnion(DataMap unionMember) {
    String recordType = unionMember.keySet().stream().findFirst().get();
    return RecordUtils.toRecordTemplate(recordType, unionMember.getDataMap(recordType));
  }
}

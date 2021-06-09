package com.linkedin.metadata.extractor;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.DataMap;
import com.linkedin.data.element.DataElement;
import com.linkedin.data.it.IterationOrder;
import com.linkedin.data.it.ObjectIterator;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.models.FieldSpec;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;


/**
 * Extracts fields from a RecordTemplate based on the appropriate {@link FieldSpec}.
 */
public class SnapshotToAspectMap {
  private SnapshotToAspectMap() {
  }

  /**
   * Function to extract the fields that match the input fieldSpecs
   */
  public static Map<String, DataMap> extractAspectMap(RecordTemplate snapshot) {

    final ObjectIterator iterator = new ObjectIterator(snapshot.data(), snapshot.schema(), IterationOrder.PRE_ORDER);
    final Map<String, DataMap> aspectsByName = new HashMap<>();

    for (DataElement dataElement = iterator.next(); dataElement != null; dataElement = iterator.next()) {
      final PathSpec pathSpec = dataElement.getSchemaPathSpec();
      List<String> pathComponents = pathSpec.getPathComponents();
      // three components representing /aspect/*/<aspectClassName>
      if (pathComponents.size() != 3) {
        continue;
      }
      String aspectName = PegasusUtils.getAspectNameFromFullyQualifiedName(pathComponents.get(2));
      aspectsByName.put(aspectName, (DataMap) dataElement.getValue());
    }

    return aspectsByName;
  }
}

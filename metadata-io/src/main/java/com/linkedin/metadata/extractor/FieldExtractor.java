package com.linkedin.metadata.extractor;

import com.linkedin.data.DataMap;
import com.linkedin.data.element.DataElement;
import com.linkedin.data.it.IterationOrder;
import com.linkedin.data.it.ObjectIterator;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EntitySpecBuilder;
import com.linkedin.metadata.models.FieldSpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import org.apache.commons.lang3.StringUtils;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FieldExtractor {
    private FieldExtractor() {
    }

    private static <T extends FieldSpec> Map<String, T> getPathToFieldSpecMap(
            Map<String, List<T>> fieldSpecsPerAspect) {
        return fieldSpecsPerAspect.entrySet().stream().flatMap(entry -> entry.getValue().stream()
                .map(fieldSpec -> entry.getKey() + "/" + fieldSpec.getPath().toString()))
                .collect(Collectors.toMap(spec -> spec.getPath().toString(), Function.identity()));
    }

    /**
     * Function to extract the fields that match the input fieldSpecs
     */
    public static <T extends FieldSpec> Map<T, DataMap> extractFields(RecordTemplate snapshot,
                                                                      Map<String, List<T>> fieldSpecsPerAspect) {
        final EntitySpec entitySpec = EntitySpecBuilder.buildEntitySpec(snapshot.schema());
        ObjectIterator dataElement = new ObjectIterator(snapshot.data(),
                snapshot.schema(),
                IterationOrder.PRE_ORDER);
        Map<String, T> pathToFieldSpec = getPathToFieldSpecMap(fieldSpecsPerAspect);
        final Map<T, DataMap> result = new HashMap<>();
        while (true) {
            final DataElement next = dataElement.next();
            final PathSpec pathSpec = next.getSchemaPathSpec();
            List<String> pathComponents = pathSpec.getPathComponents();
            if (pathComponents.size() < 4) {
                continue;
            }
            final String path = StringUtils.join(pathComponents.subList(2, pathComponents.size()), "/");
            final Optional<T> matchingSpec = Optional.ofNullable(pathToFieldSpec.get(suffix));
            matchingSpec.get().getPegasusSchema().getType()
            matchingSpec.ifPresent(spec -> result.put(spec, ));
            if (matchingSpec.isPresent()) {
                result.put(matc)
            }
            if (next == null) {
                break;
            }
        }
    }
}

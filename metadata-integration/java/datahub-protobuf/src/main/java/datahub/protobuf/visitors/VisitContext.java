package datahub.protobuf.visitors;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.DatasetUrn;
import datahub.protobuf.model.FieldTypeEdge;
import datahub.protobuf.model.ProtobufElement;
import datahub.protobuf.model.ProtobufField;
import datahub.protobuf.model.ProtobufGraph;
import datahub.protobuf.model.ProtobufMessage;
import lombok.Builder;
import lombok.Getter;
import org.jgrapht.GraphPath;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Builder
@Getter
public class VisitContext {
    public static final String FIELD_PATH_VERSION = "[version=2.0]";

    private final DatasetUrn datasetUrn;
    private final ProtobufGraph graph;
    private final AuditStamp auditStamp;

    public ProtobufMessage root() {
        return graph.root();
    }

    public Stream<GraphPath<ProtobufElement, FieldTypeEdge>> streamAllPaths(ProtobufField field) {
        return graph.getAllPaths(root(), field).stream();
    }

    public String getFieldPath(GraphPath<ProtobufElement, FieldTypeEdge> path) {
        String fieldPathString = path.getEdgeList().stream()
                .flatMap(e -> Stream.of(e.getType(), e.getEdgeTarget().name()))
                .collect(Collectors.joining("."));
        return String.join(".", FIELD_PATH_VERSION, root().fieldPathType(), fieldPathString);
    }

    // This is because order matters for the frontend. Both for matching the protobuf field order
    // and also the nested struct's fieldPaths
    public Double calculateSortOrder(GraphPath<ProtobufElement, FieldTypeEdge> path, ProtobufField field) {
        List<Integer> weights = path.getEdgeList().stream()
                .map(FieldTypeEdge::getEdgeTarget)
                .filter(f -> f instanceof ProtobufField)
                .map(f -> ((ProtobufField) f).sortWeight())
                .collect(Collectors.toList());

        return IntStream.range(0, weights.size())
                .mapToDouble(i -> weights.get(i) * (1.0 / (i + 1)))
                .reduce(Double::sum)
                .orElse(0);
    }

    public static class VisitContextBuilder {

    };
}

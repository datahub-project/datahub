package datahub.protobuf.visitors;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import datahub.protobuf.model.FieldTypeEdge;
import datahub.protobuf.model.ProtobufElement;
import datahub.protobuf.model.ProtobufGraph;
import org.jgrapht.GraphPath;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static datahub.protobuf.TestFixtures.getTestProtobufFileSet;
import static datahub.protobuf.TestFixtures.getTestProtobufGraph;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class VisitContextTest {

    @Test
    public void duplicateNestedTest() throws IOException {
        FileDescriptorSet fileset = getTestProtobufFileSet("protobuf", "messageB");
        ProtobufGraph graph = getTestProtobufGraph("protobuf", "messageB");
        VisitContext test = VisitContext.builder().graph(graph).build();

        List<ProtobufElement> nestedMessages = graph.vertexSet().stream().filter(f -> f.name().endsWith("nested"))
                .collect(Collectors.toList());

        List<GraphPath<ProtobufElement, FieldTypeEdge>> nestedPathsA = graph.getAllPaths(graph.root(), nestedMessages.get(0));
        List<GraphPath<ProtobufElement, FieldTypeEdge>> nestedPathsB = graph.getAllPaths(graph.root(), nestedMessages.get(1));
        assertNotEquals(nestedPathsA, nestedPathsB);

        Set<String> fieldPathsA = nestedPathsA.stream().map(test::getFieldPath).collect(Collectors.toSet());
        Set<String> fieldPathsB = nestedPathsB.stream().map(test::getFieldPath).collect(Collectors.toSet());
        assertNotEquals(fieldPathsA, fieldPathsB);
    }
}

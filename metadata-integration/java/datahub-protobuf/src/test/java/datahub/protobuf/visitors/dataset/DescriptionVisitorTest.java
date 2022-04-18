package datahub.protobuf.visitors.dataset;

import datahub.protobuf.model.ProtobufGraph;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static datahub.protobuf.TestFixtures.*;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class DescriptionVisitorTest {

    @Test
    public void visitorTest() throws IOException {
        ProtobufGraph graph = getTestProtobufGraph("protobuf", "messageB");

        DescriptionVisitor test = new DescriptionVisitor();

        assertEquals(Set.of("This contains nested types.\n\nOwned by TeamB"),
                graph.accept(getVisitContextBuilder("protobuf.MessageB"), List.of(test)).collect(Collectors.toSet()));
    }
}

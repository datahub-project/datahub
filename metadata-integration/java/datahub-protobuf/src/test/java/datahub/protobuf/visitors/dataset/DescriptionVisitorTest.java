package datahub.protobuf.visitors.dataset;

import static datahub.protobuf.TestFixtures.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import datahub.protobuf.model.ProtobufGraph;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class DescriptionVisitorTest {

  @Test
  public void visitorTest() throws IOException {
    ProtobufGraph graph = getTestProtobufGraph("protobuf", "messageC2", "protobuf.MessageC2");

    DescriptionVisitor test = new DescriptionVisitor();

    assertEquals(
        Set.of("This contains nested type\n\nDescription for MessageC2"),
        graph
            .accept(getVisitContextBuilder("protobuf.MessageC2"), List.of(test))
            .collect(Collectors.toSet()));
  }
}

package datahub.protobuf.visitors.dataset;

import static datahub.protobuf.TestFixtures.getTestProtobufGraph;
import static datahub.protobuf.TestFixtures.getVisitContextBuilder;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.Urn;
import datahub.protobuf.model.ProtobufGraph;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class DomainVisitorTest {

  @Test
  public void visitorTest() throws IOException {
    ProtobufGraph graph = getTestProtobufGraph("extended_protobuf", "messageA");

    DomainVisitor test = new DomainVisitor();

    assertEquals(
        Set.of(Urn.createFromTuple("domain", "engineering")),
        graph
            .accept(getVisitContextBuilder("extended_protobuf.MessageA"), List.of(test))
            .collect(Collectors.toSet()));
  }
}

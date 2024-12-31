package datahub.protobuf.visitors.dataset;

import static datahub.protobuf.TestFixtures.getTestProtobufGraph;
import static datahub.protobuf.TestFixtures.getVisitContextBuilder;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.urn.GlossaryTermUrn;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class TermAssociationVisitorTest {

  @Test
  public void extendedMessageTest() throws IOException {
    TermAssociationVisitor test = new TermAssociationVisitor();
    assertEquals(
        Set.of(
            new GlossaryTermAssociation().setUrn(new GlossaryTermUrn("a")),
            new GlossaryTermAssociation().setUrn(new GlossaryTermUrn("b")),
            new GlossaryTermAssociation().setUrn(new GlossaryTermUrn("MetaEnumExample.ENTITY")),
            new GlossaryTermAssociation().setUrn(new GlossaryTermUrn("MetaEnumExample.EVENT")),
            new GlossaryTermAssociation()
                .setUrn(new GlossaryTermUrn("Classification.HighlyConfidential"))),
        getTestProtobufGraph("extended_protobuf", "messageA")
            .accept(getVisitContextBuilder("extended_protobuf.Person"), List.of(test))
            .collect(Collectors.toSet()));
  }

  @Test
  public void extendedFieldTest() throws IOException {
    TermAssociationVisitor test = new TermAssociationVisitor();
    assertEquals(
        Set.of(),
        getTestProtobufGraph("extended_protobuf", "messageB")
            .accept(getVisitContextBuilder("extended_protobuf.Person"), List.of(test))
            .collect(Collectors.toSet()));
  }
}

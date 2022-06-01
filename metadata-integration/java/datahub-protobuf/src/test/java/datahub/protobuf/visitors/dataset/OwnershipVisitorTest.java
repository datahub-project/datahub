package datahub.protobuf.visitors.dataset;

import com.linkedin.common.Owner;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import datahub.protobuf.model.ProtobufGraph;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static datahub.protobuf.TestFixtures.getTestProtobufGraph;
import static datahub.protobuf.TestFixtures.getVisitContextBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class OwnershipVisitorTest {

    @Test
    public void visitorTest() throws IOException {
        ProtobufGraph graph = getTestProtobufGraph("extended_protobuf", "messageA");

        OwnershipVisitor test = new OwnershipVisitor();

        assertEquals(Set.of(new Owner()
                                .setType(OwnershipType.PRODUCER)
                                .setSource(new OwnershipSource().setType(OwnershipSourceType.MANUAL))
                                .setOwner(Urn.createFromTuple("corpgroup", "teamb")),
                        new Owner()
                                .setType(OwnershipType.PRODUCER)
                                .setSource(new OwnershipSource().setType(OwnershipSourceType.MANUAL))
                                .setOwner(Urn.createFromTuple("corpuser", "datahub")),
                        new Owner()
                                .setType(OwnershipType.TECHNICAL_OWNER)
                                .setSource(new OwnershipSource().setType(OwnershipSourceType.MANUAL))
                                .setOwner(Urn.createFromTuple("corpgroup", "technicalowner"))
                ),
                graph.accept(getVisitContextBuilder("extended_protobuf.MessageA"), List.of(test)).collect(Collectors.toSet()));
    }

    @Test
    public void visitorSingleOwnerTest() throws IOException {
        ProtobufGraph graph = getTestProtobufGraph("extended_protobuf", "messageB");

        OwnershipVisitor test = new OwnershipVisitor();

        assertEquals(Set.of(new Owner()
                                .setType(OwnershipType.DATA_STEWARD)
                                .setSource(new OwnershipSource().setType(OwnershipSourceType.MANUAL))
                                .setOwner(Urn.createFromTuple("corpuser", "datahub"))
                ),
                graph.accept(getVisitContextBuilder("extended_protobuf.MessageB"), List.of(test)).collect(Collectors.toSet()));
    }
}

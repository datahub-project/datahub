package datahub.protobuf.visitors.tag;

import com.linkedin.tag.TagProperties;
import datahub.protobuf.visitors.tags.ProtobufExtensionTagVisitor;
import datahub.event.MetadataChangeProposalWrapper;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static datahub.protobuf.TestFixtures.getTestProtobufGraph;
import static datahub.protobuf.TestFixtures.getVisitContextBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class ProtobufExtensionTagVisitorTest {

    @Test
    public void extendedMessageTest() throws IOException {
        ProtobufExtensionTagVisitor test = new ProtobufExtensionTagVisitor();
        assertEquals(Set.of(
                new TagProperties()
                        .setName("team.TeamB")
                        .setDescription("meta.msg.team"),
                new TagProperties()
                        .setName("bool_feature")
                        .setDescription("meta.msg.bool_feature is true."),
                new TagProperties()
                        .setName("MetaEnumExample.ENTITY")
                        .setDescription("Enum MetaEnumExample.ENTITY of {UNKNOWN, ENTITY, EVENT}")
        ), getTestProtobufGraph("extended_protobuf", "messageA")
                .accept(getVisitContextBuilder("extended_protobuf.Person"), List.of(test))
                .map(MetadataChangeProposalWrapper::getAspect)
                .collect(Collectors.toSet()));
    }

    @Test
    public void extendedFieldTest() throws IOException {
        Set<TagProperties> expectedTagProperties = Set.of(
                new TagProperties()
                        .setName("product_type_bool")
                        .setDescription("meta.fld.product_type_bool is true."),
                new TagProperties()
                        .setName("product_type.my type")
                        .setDescription("meta.fld.product_type"),
                new TagProperties()
                        .setName("MetaEnumExample.EVENT")
                        .setDescription("Enum MetaEnumExample.EVENT of {UNKNOWN, ENTITY, EVENT}")
        );

        assertEquals(expectedTagProperties,
                getTestProtobufGraph("extended_protobuf", "messageB")
                        .accept(getVisitContextBuilder("extended_protobuf.Person"), List.of(new ProtobufExtensionTagVisitor()))
                        .map(MetadataChangeProposalWrapper::getAspect)
                        .collect(Collectors.toSet()));
    }
}
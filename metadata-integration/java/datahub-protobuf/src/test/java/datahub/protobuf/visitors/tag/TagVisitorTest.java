package datahub.protobuf.visitors.tag;

import static datahub.protobuf.TestFixtures.getTestProtobufGraph;
import static datahub.protobuf.TestFixtures.getVisitContextBuilder;
import static org.testng.Assert.assertEquals;

import com.linkedin.tag.TagProperties;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.protobuf.visitors.tags.TagVisitor;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class TagVisitorTest {

  @Test
  public void extendedMessageTest() throws IOException {
    TagVisitor test = new TagVisitor();
    assertEquals(
        Set.of(
            new TagProperties()
                .setName("bool_feature")
                .setDescription("meta.msg.bool_feature is true."),
            new TagProperties()
                .setName("MetaEnumExample.ENTITY")
                .setDescription("Enum MetaEnumExample.ENTITY of {UNKNOWN, ENTITY, EVENT}"),
            new TagProperties()
                .setName("MetaEnumExample.EVENT")
                .setDescription("Enum MetaEnumExample.EVENT of {UNKNOWN, ENTITY, EVENT}"),
            new TagProperties().setName("a").setDescription("meta.msg.tag_list"),
            new TagProperties().setName("b").setDescription("meta.msg.tag_list"),
            new TagProperties().setName("c").setDescription("meta.msg.tag_list"),
            new TagProperties().setName("repeat_string.a").setDescription("meta.msg.repeat_string"),
            new TagProperties().setName("repeat_string.b").setDescription("meta.msg.repeat_string"),
            new TagProperties().setName("deprecated").setColorHex("#FF0000")),
        getTestProtobufGraph("extended_protobuf", "messageA")
            .accept(getVisitContextBuilder("extended_protobuf.Person"), List.of(test))
            .map(MetadataChangeProposalWrapper::getAspect)
            .collect(Collectors.toSet()));
  }

  @Test
  public void extendedFieldTest() throws IOException {
    Set<TagProperties> expectedTagProperties =
        Set.of(
            new TagProperties()
                .setName("product_type_bool")
                .setDescription("meta.fld.product_type_bool is true."),
            new TagProperties()
                .setName("product_type.my type")
                .setDescription("meta.fld.product_type"),
            new TagProperties()
                .setName("MetaEnumExample.EVENT")
                .setDescription("Enum MetaEnumExample.EVENT of {UNKNOWN, ENTITY, EVENT}"),
            new TagProperties().setName("d").setDescription("meta.fld.tag_list"),
            new TagProperties().setName("e").setDescription("meta.fld.tag_list"),
            new TagProperties().setName("f").setDescription("meta.fld.tag_list"),
            new TagProperties().setName("deprecated").setColorHex("#FF0000"));

    assertEquals(
        expectedTagProperties,
        getTestProtobufGraph("extended_protobuf", "messageB")
            .accept(getVisitContextBuilder("extended_protobuf.Person"), List.of(new TagVisitor()))
            .map(MetadataChangeProposalWrapper::getAspect)
            .collect(Collectors.toSet()));
  }
}

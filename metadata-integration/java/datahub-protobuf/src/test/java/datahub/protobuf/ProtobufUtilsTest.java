package datahub.protobuf;

import static datahub.protobuf.TestFixtures.getTestProtobufFileSet;
import static datahub.protobuf.TestFixtures.getTestProtoc;
import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.ExtensionRegistry;
import datahub.protobuf.model.ProtobufGraph;
import java.io.IOException;
import org.junit.jupiter.api.Test;

public class ProtobufUtilsTest {

  @Test
  public void registryTest() throws IOException, IllegalArgumentException {
    byte[] protocBytes = getTestProtoc("extended_protobuf", "messageA").readAllBytes();
    DescriptorProtos.FileDescriptorSet fileSet =
        getTestProtobufFileSet("extended_protobuf", "messageA");
    ExtensionRegistry registry = ProtobufUtils.buildRegistry(fileSet);
    DescriptorProtos.FileDescriptorSet fileSetWithRegistry =
        DescriptorProtos.FileDescriptorSet.parseFrom(protocBytes, registry);

    assertNotEquals(fileSet, fileSetWithRegistry);

    /*
     *
     * Without the ExtensionRegistry we get field numbers instead of the names.
     */
    ProtobufGraph graph = new ProtobufGraph(fileSet, null);
    assertEquals(
        "[meta.msg.classification_enum]: HighlyConfidential\n"
            + "[meta.msg.team]: \"corpGroup:TeamB\"\n"
            + "[meta.msg.team]: \"corpUser:datahub\"\n"
            + "[meta.msg.technical_owner]: \"corpGroup:TechnicalOwner\"\n"
            + "[meta.msg.domain]: \"Engineering\"\n"
            + "[meta.msg.type]: ENTITY\n"
            + "[meta.msg.bool_feature]: true\n"
            + "[meta.msg.alert_channel]: \"#alerts\"\n"
            + "[meta.msg.tag_list]: \"a, b, c\"\n"
            + "[meta.msg.repeat_string]: \"a\"\n"
            + "[meta.msg.repeat_string]: \"b\"\n"
            + "[meta.msg.repeat_enum]: ENTITY\n"
            + "[meta.msg.repeat_enum]: EVENT\n",
        graph.root().messageProto().getOptions().toString());
  }
}

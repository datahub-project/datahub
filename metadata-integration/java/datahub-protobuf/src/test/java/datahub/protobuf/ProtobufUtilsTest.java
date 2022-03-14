package datahub.protobuf;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.ExtensionRegistry;
import datahub.protobuf.model.ProtobufGraph;
import org.junit.Test;

import java.io.IOException;

import static datahub.protobuf.TestFixtures.getTestProtobufFileSet;
import static datahub.protobuf.TestFixtures.getTestProtoc;
import static org.junit.jupiter.api.Assertions.*;


public class ProtobufUtilsTest {

    @Test
    public void registryTest() throws IOException, IllegalArgumentException {
        byte[] protocBytes = getTestProtoc("extended_protobuf", "messageA").readAllBytes();
        DescriptorProtos.FileDescriptorSet fileSet = getTestProtobufFileSet("extended_protobuf", "messageA");
        ExtensionRegistry registry = ProtobufUtils.buildRegistry(fileSet);
        DescriptorProtos.FileDescriptorSet fileSetWithRegistry = DescriptorProtos.FileDescriptorSet.parseFrom(protocBytes, registry);

        assertNotEquals(fileSet, fileSetWithRegistry);

        /*
         *
         * Without the ExtensionRegistry we get field numbers instead of the names.
         */
        ProtobufGraph graph = new ProtobufGraph(fileSet, null);
        assertEquals("[meta.msg.classification_enum]: HighlyConfidential\n"
                + "[meta.msg.team]: \"TeamB\"\n"
                + "[meta.msg.type]: ENTITY\n"
                + "[meta.msg.bool_feature]: true\n"
                + "[meta.msg.alert_channel]: \"#alerts\"\n", graph.root().messageProto().getOptions().toString());
    }
}

package datahub.protobuf.visitors.dataset;

import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static datahub.protobuf.TestFixtures.getTestProtobufGraph;
import static datahub.protobuf.TestFixtures.getVisitContextBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class ProtobufExtensionPropertyVisitorTest {

    @Test
    public void extendedMessageTest() throws IOException {
        ProtobufExtensionPropertyVisitor test = new ProtobufExtensionPropertyVisitor();

        List<DatasetProperties> actual = getTestProtobufGraph("extended_protobuf", "messageA")
                .accept(getVisitContextBuilder("extended_protobuf.Person"),
                        List.of(test)).collect(Collectors.toList());

        assertEquals(List.of(
                new DatasetProperties().setCustomProperties(new StringMap(Map.of("classification_enum", "HighlyConfidential",
                        "bool_feature", "true",
                        "alert_channel", "#alerts",
                        "repeat_enum", "[\"ENTITY\",\"EVENT\"]",
                        "team", "[\"corpGroup:TeamB\",\"corpUser:datahub\"]",
                        "technical_owner", "[\"corpGroup:TechnicalOwner\"]",
                        "tag_list", "a, b, c",
                        "domain", "Engineering",
                        "repeat_string", "[\"a\",\"b\"]",
                        "type", "ENTITY")))),
                actual);
    }

    @Test
    public void extendedFieldTest() throws IOException {
        ProtobufExtensionPropertyVisitor test = new ProtobufExtensionPropertyVisitor();
        List<DatasetProperties> actual = getTestProtobufGraph("extended_protobuf", "messageB")
                .accept(getVisitContextBuilder("extended_protobuf.Person"),
                        List.of(test)).collect(Collectors.toList());

        assertEquals(List.of(new DatasetProperties()
                        .setCustomProperties(new StringMap(Map.of("data_steward", "corpUser:datahub")))), actual);
    }
}

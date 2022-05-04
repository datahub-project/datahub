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
import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class PropertyVisitorTest {

    @Test
    public void extendedMessageTest() throws IOException {
        PropertyVisitor test = new PropertyVisitor();

        List<DatasetProperties> actual = getTestProtobufGraph("extended_protobuf", "messageA")
                .accept(getVisitContextBuilder("extended_protobuf.Person"),
                        List.of(test)).collect(Collectors.toList());

        assertEquals(List.of(
                new DatasetProperties().setCustomProperties(new StringMap(Map.ofEntries(
                        entry("classification_enum", "HighlyConfidential"),
                        entry("bool_feature", "true"),
                        entry("alert_channel", "#alerts"),
                        entry("repeat_enum", "[\"ENTITY\",\"EVENT\"]"),
                        entry("team", "[\"corpGroup:TeamB\",\"corpUser:datahub\"]"),
                        entry("technical_owner", "[\"corpGroup:TechnicalOwner\"]"),
                        entry("tag_list", "a, b, c"),
                        entry("domain", "Engineering"),
                        entry("repeat_string", "[\"a\",\"b\"]"),
                        entry("type", "ENTITY"))))),
                actual);
    }

    @Test
    public void extendedFieldTest() throws IOException {
        PropertyVisitor test = new PropertyVisitor();
        List<DatasetProperties> actual = getTestProtobufGraph("extended_protobuf", "messageB")
                .accept(getVisitContextBuilder("extended_protobuf.Person"),
                        List.of(test)).collect(Collectors.toList());

        assertEquals(List.of(new DatasetProperties()
                        .setCustomProperties(new StringMap(Map.ofEntries(
                                entry("data_steward", "corpUser:datahub"),
                                entry("deprecated", "true"),
                                entry("deprecation_note", "[\"Deprecated for this other message.\",\"Drop in replacement.\"]"),
                                entry("deprecation_time", "1649689387")
                        )))), actual);
    }
}

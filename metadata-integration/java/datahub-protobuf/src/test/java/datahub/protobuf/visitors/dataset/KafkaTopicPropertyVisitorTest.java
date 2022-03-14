package datahub.protobuf.visitors.dataset;

import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static datahub.protobuf.TestFixtures.getTestProtobufGraph;
import static datahub.protobuf.TestFixtures.getVisitContextBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class KafkaTopicPropertyVisitorTest {

    @Test
    public void visitorTest() throws IOException {
        KafkaTopicPropertyVisitor test = new KafkaTopicPropertyVisitor();
        assertEquals(List.of(new DatasetProperties()
                        .setCustomProperties(new StringMap(Map.of("kafka_topic", "platform.topic")))),
                getTestProtobufGraph("protobuf", "messageA")
                        .accept(getVisitContextBuilder("MessageB"),
                                List.of(test)).collect(Collectors.toList()));
    }

    @Test
    public void visitorEmptyTest() throws IOException {
        KafkaTopicPropertyVisitor test = new KafkaTopicPropertyVisitor();
        assertEquals(Set.of(), getTestProtobufGraph("protobuf", "messageB")
                .accept(getVisitContextBuilder("MessageB"), List.of(test)).collect(Collectors.toSet()));
    }
}

package datahub.protobuf.visitors.dataset;

import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.url.Url;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static datahub.protobuf.TestFixtures.*;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class InstitutionalMemoryVisitorTest {

    @Test
    public void messageATest() throws IOException {
        InstitutionalMemoryVisitor test = new InstitutionalMemoryVisitor("SLACK123", "myOrg");
        assertEquals(Set.of(new InstitutionalMemoryMetadata()
                                .setCreateStamp(TEST_AUDIT_STAMP)
                                .setDescription("Slack Channel")
                                .setUrl(new Url("https://slack.com/app_redirect?channel=test-slack&team=SLACK123")),
                        new InstitutionalMemoryMetadata()
                                .setCreateStamp(TEST_AUDIT_STAMP)
                                .setDescription("Github Team")
                                .setUrl(new Url("https://github.com/orgs/myOrg/teams/teama")),
                        new InstitutionalMemoryMetadata()
                                .setCreateStamp(TEST_AUDIT_STAMP)
                                .setDescription("MessageA Reference 1")
                                .setUrl(new Url("https://some/link")),
                        new InstitutionalMemoryMetadata()
                                .setCreateStamp(TEST_AUDIT_STAMP)
                                .setDescription("MessageA Reference 2")
                                .setUrl(new Url("https://www.google.com/search?q=protobuf+messages")),
                        new InstitutionalMemoryMetadata()
                                .setCreateStamp(TEST_AUDIT_STAMP)
                                .setDescription("MessageA Reference 3")
                                .setUrl(new Url("https://github.com/apache/kafka")),
                        new InstitutionalMemoryMetadata()
                                .setCreateStamp(TEST_AUDIT_STAMP)
                                .setDescription("MessageA.map_field Reference 1")
                                .setUrl(new Url("https://developers.google.com/protocol-buffers/docs/proto3#maps"))
                        ),

                getTestProtobufGraph("protobuf", "messageA")
                        .accept(getVisitContextBuilder("protobuf.MessageA"),
                        List.of(test)).collect(Collectors.toSet()));
    }

    @Test
    public void messageBTest() throws IOException {
        InstitutionalMemoryVisitor test = new InstitutionalMemoryVisitor("SLACK123", "myOrg");
        assertEquals(Set.of(),
                getTestProtobufGraph("protobuf", "messageB")
                        .accept(getVisitContextBuilder("protobuf.MessageB"),
                                List.of(test)).collect(Collectors.toSet()));
    }

    @Test
    public void messageCTest() throws IOException {
        InstitutionalMemoryVisitor test = new InstitutionalMemoryVisitor("SLACK123", "myOrg");
        assertEquals(Set.of(), getTestProtobufGraph("protobuf", "messageC")
                .accept(getVisitContextBuilder("protobuf.MessageC"),
                        List.of(test)).collect(Collectors.toSet()));
    }
}

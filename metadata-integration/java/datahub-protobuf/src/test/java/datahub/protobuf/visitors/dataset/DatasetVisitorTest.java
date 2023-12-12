package datahub.protobuf.visitors.dataset;

import static datahub.protobuf.TestFixtures.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.template.RecordTemplate;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.protobuf.ProtobufDataset;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.VisitContext;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class DatasetVisitorTest {

  @Test
  public void protocBase64Test() throws URISyntaxException, IOException {
    String expected = "23454345452345233455";
    DatasetVisitor test = DatasetVisitor.builder().protocBase64(expected).build();

    List<MetadataChangeProposalWrapper<? extends RecordTemplate>> changes =
        test.visitGraph(
                VisitContext.builder()
                    .auditStamp(TEST_AUDIT_STAMP)
                    .datasetUrn(
                        DatasetUrn.createFromString(
                            "urn:li:dataset:(urn:li:dataPlatform:kafka,protobuf.MessageA,TEST)"))
                    .graph(getTestProtobufGraph("protobuf", "messageA"))
                    .build())
            .collect(Collectors.toList());

    assertEquals(expected, extractCustomProperty(changes.get(0), "protoc"));
  }

  @Test
  public void customDescriptionVisitors() throws IOException {
    ProtobufDataset testDataset = getTestProtobufDataset("protobuf", "messageA");

    DatasetVisitor test =
        DatasetVisitor.builder()
            .descriptionVisitor(
                new ProtobufModelVisitor<String>() {
                  @Override
                  public Stream<String> visitGraph(VisitContext context) {
                    return Stream.of("Test Description");
                  }
                })
            .build();
    testDataset.setDatasetVisitor(test);

    assertEquals(
        "Test Description", extractAspect(testDataset.getDatasetMCPs().get(0), "description"));
  }
}

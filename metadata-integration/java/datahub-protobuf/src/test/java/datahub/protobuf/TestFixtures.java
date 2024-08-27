package datahub.protobuf;

import com.google.protobuf.DescriptorProtos;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.protobuf.model.ProtobufGraph;
import datahub.protobuf.visitors.VisitContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class TestFixtures {
  private TestFixtures() {}

  public static final DataPlatformUrn TEST_DATA_PLATFORM = new DataPlatformUrn("kafka");
  public static final AuditStamp TEST_AUDIT_STAMP =
      new AuditStamp().setTime(System.currentTimeMillis()).setActor(new CorpuserUrn("datahub"));

  public static InputStream getTestProtoc(String protoPackage, String filename) {
    return Objects.requireNonNull(
        TestFixtures.class
            .getClassLoader()
            .getResourceAsStream(String.format("%s/%s.protoc", protoPackage, filename)));
  }

  public static String getTestProtoSource(String protoPackage, String filename) throws IOException {
    return new String(
        Objects.requireNonNull(
                TestFixtures.class
                    .getClassLoader()
                    .getResourceAsStream(String.format("%s/%s.proto", protoPackage, filename)))
            .readAllBytes(),
        StandardCharsets.UTF_8);
  }

  public static ProtobufDataset getTestProtobufDataset(String protoPackage, String filename)
      throws IOException {
    return ProtobufDataset.builder()
        .setDataPlatformUrn(TEST_DATA_PLATFORM)
        .setSchema(getTestProtoSource(protoPackage, filename))
        .setProtocIn(getTestProtoc(protoPackage, filename))
        .setAuditStamp(TEST_AUDIT_STAMP)
        .setFabricType(FabricType.TEST)
        .setGithubOrganization("myOrg")
        .setSlackTeamId("SLACK123")
        .build();
  }

  public static DescriptorProtos.FileDescriptorSet getTestProtobufFileSet(
      String protoPackage, String filename) throws IOException {
    return DescriptorProtos.FileDescriptorSet.parseFrom(
        getTestProtoc(protoPackage, filename).readAllBytes());
  }

  public static VisitContext.VisitContextBuilder getVisitContextBuilder(String message) {
    return VisitContext.builder()
        .datasetUrn(new DatasetUrn(TEST_DATA_PLATFORM, message, FabricType.TEST))
        .auditStamp(TEST_AUDIT_STAMP);
  }

  public static ProtobufGraph getTestProtobufGraph(String protoPackage, String filename)
      throws IOException {
    return new ProtobufGraph(getTestProtobufFileSet(protoPackage, filename));
  }

  public static ProtobufGraph getTestProtobufGraph(
      String protoPackage, String filename, String messageName) throws IOException {
    return new ProtobufGraph(getTestProtobufFileSet(protoPackage, filename), messageName);
  }

  public static Object extractAspect(
      MetadataChangeProposalWrapper<? extends RecordTemplate> mcp, String aspect) {
    return mcp.getAspect().data().get(aspect);
  }

  public static Object extractCustomProperty(
      MetadataChangeProposalWrapper<? extends RecordTemplate> mcp, String key) {
    return ((DataMap) extractAspect(mcp, "customProperties")).get(key);
  }

  public static String extractDocumentSchema(ProtobufDataset protobufDataset) {
    return String.valueOf(
        ((DataMap)
                ((DataMap) protobufDataset.getSchemaMetadata().getPlatformSchema().data())
                    .get("com.linkedin.schema.KafkaSchema"))
            .get("documentSchema"));
  }
}

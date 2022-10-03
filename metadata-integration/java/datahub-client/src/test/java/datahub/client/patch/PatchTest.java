package datahub.client.patch;

import com.linkedin.common.FabricType;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.MetadataWriteResponse;
import datahub.client.file.FileEmitter;
import datahub.client.file.FileEmitterConfig;
import datahub.client.patch.common.OwnershipPatchBuilder;
import datahub.client.patch.dataset.EditableSchemaMetadataPatchBuilder;
import datahub.client.patch.dataset.UpstreamLineagePatchBuilder;
import datahub.client.rest.RestEmitter;
import datahub.client.rest.RestEmitterConfig;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class PatchTest {

  /**
   * Examples for running patches, tests set to ignore as they target a GMS running on localhost
   */

  @Test
  @Ignore
  public void testLocalUpstream() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {
      MetadataChangeProposal upstreamPatch = new UpstreamLineagePatchBuilder().op(PatchOperationType.ADD)
          .urn(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"))
          .dataset(DatasetUrn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"))
          .lineageType(DatasetLineageType.TRANSFORMED).build();
      Future<MetadataWriteResponse> response = restEmitter.emit(upstreamPatch);

      System.out.println(response.get().getResponseContent());

    } catch (URISyntaxException | IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalEditableSchemaMetadataTag() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {
      TagAssociation tagAssociation = new TagAssociation();
      tagAssociation.setTag(new TagUrn("Legacy"));
      MetadataChangeProposal fieldTagPatch = new EditableSchemaMetadataPatchBuilder("field_foo")
          .urn(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"))
          .op(PatchOperationType.ADD)
          .tag(tagAssociation)
          .build();
      Future<MetadataWriteResponse> response = restEmitter.emit(fieldTagPatch);

      System.out.println(response.get().getResponseContent());

    } catch (IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalEditableSchemaMetadataTerm() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {

      GlossaryTermAssociation termAssociation = new GlossaryTermAssociation();
      termAssociation.setUrn(new GlossaryTermUrn("CustomerAccount"));
      MetadataChangeProposal fieldTermPatch = new EditableSchemaMetadataPatchBuilder("field_foo")
          .urn(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"))
          .op(PatchOperationType.ADD)
          .glossaryTerm(termAssociation)
          .build();
      Future<MetadataWriteResponse> response = restEmitter.emit(fieldTermPatch);

      System.out.println(response.get().getResponseContent());

    } catch (IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalOwnership() {
    FileEmitter fileEmitter = new FileEmitter(FileEmitterConfig.builder()
        .fileName("test_mcp.json").build());
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {

      DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn("hive"), "SampleHiveDataset", FabricType.PROD);
      MetadataChangeProposal ownershipPatch = new OwnershipPatchBuilder()
          .urn(datasetUrn)
          .op(PatchOperationType.ADD)
          .owner(new CorpuserUrn("gdoe"))
          .ownershipType(OwnershipType.DATAOWNER)
          .build();
      System.out.println(ownershipPatch.toString());
      Future<MetadataWriteResponse> response = fileEmitter.emit(ownershipPatch);
      response.get();
      response = restEmitter.emit(ownershipPatch);
      System.out.println(response.get().getResponseContent());
      fileEmitter.close();

    } catch (IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

}

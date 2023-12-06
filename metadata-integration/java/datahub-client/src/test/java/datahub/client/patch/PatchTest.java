package datahub.client.patch;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Edge;
import com.linkedin.common.FabricType;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.MetadataWriteResponse;
import datahub.client.file.FileEmitter;
import datahub.client.file.FileEmitterConfig;
import datahub.client.patch.common.OwnershipPatchBuilder;
import datahub.client.patch.dataflow.DataFlowInfoPatchBuilder;
import datahub.client.patch.datajob.DataJobInfoPatchBuilder;
import datahub.client.patch.datajob.DataJobInputOutputPatchBuilder;
import datahub.client.patch.dataset.DatasetPropertiesPatchBuilder;
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

  /** Examples for running patches, tests set to ignore as they target a GMS running on localhost */
  @Test
  @Ignore
  public void testLocalUpstream() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {
      MetadataChangeProposal upstreamPatch =
          new UpstreamLineagePatchBuilder()
              .urn(
                  UrnUtils.getUrn(
                      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"))
              .addUpstream(
                  DatasetUrn.createFromString(
                      "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
                  DatasetLineageType.TRANSFORMED)
              .build();
      Future<MetadataWriteResponse> response = restEmitter.emit(upstreamPatch);

      System.out.println(response.get().getResponseContent());

    } catch (URISyntaxException | IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalUpstreamRemove() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {
      MetadataChangeProposal upstreamPatch =
          new UpstreamLineagePatchBuilder()
              .urn(
                  UrnUtils.getUrn(
                      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"))
              .removeUpstream(
                  DatasetUrn.createFromString(
                      "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"))
              .build();
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
      MetadataChangeProposal fieldTagPatch =
          new EditableSchemaMetadataPatchBuilder()
              .urn(
                  UrnUtils.getUrn(
                      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"))
              .addTag(tagAssociation, "field_foo")
              .build();
      Future<MetadataWriteResponse> response = restEmitter.emit(fieldTagPatch);

      System.out.println(response.get().getResponseContent());

    } catch (IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalEditableSchemaMetadataTagRemove() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {
      TagUrn urn = new TagUrn("Legacy");
      MetadataChangeProposal fieldTagPatch =
          new EditableSchemaMetadataPatchBuilder()
              .urn(
                  UrnUtils.getUrn(
                      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"))
              .removeTag(urn, "field_foo")
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
      MetadataChangeProposal fieldTermPatch =
          new EditableSchemaMetadataPatchBuilder()
              .urn(
                  UrnUtils.getUrn(
                      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"))
              .addGlossaryTerm(termAssociation, "field_foo")
              .build();
      Future<MetadataWriteResponse> response = restEmitter.emit(fieldTermPatch);

      System.out.println(response.get().getResponseContent());

    } catch (IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalEditableSchemaMetadataTermRemove() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {

      GlossaryTermUrn urn = new GlossaryTermUrn("CustomerAccount");
      MetadataChangeProposal fieldTermPatch =
          new EditableSchemaMetadataPatchBuilder()
              .urn(
                  UrnUtils.getUrn(
                      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"))
              .removeGlossaryTerm(urn, "field_foo")
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
    FileEmitter fileEmitter =
        new FileEmitter(FileEmitterConfig.builder().fileName("test_mcp.json").build());
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {

      DatasetUrn datasetUrn =
          new DatasetUrn(new DataPlatformUrn("hive"), "SampleHiveDataset", FabricType.PROD);
      MetadataChangeProposal ownershipPatch =
          new OwnershipPatchBuilder()
              .urn(datasetUrn)
              .addOwner(new CorpuserUrn("gdoe"), OwnershipType.TECHNICAL_OWNER)
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

  @Test
  @Ignore
  public void testLocalOwnershipRemove() {
    FileEmitter fileEmitter =
        new FileEmitter(FileEmitterConfig.builder().fileName("test_mcp.json").build());
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {

      DatasetUrn datasetUrn =
          new DatasetUrn(new DataPlatformUrn("hive"), "SampleHiveDataset", FabricType.PROD);
      MetadataChangeProposal ownershipPatch =
          new OwnershipPatchBuilder().urn(datasetUrn).removeOwner(new CorpuserUrn("gdoe")).build();
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

  @Test
  @Ignore
  public void testLocalOwnershipRemoveType() {
    FileEmitter fileEmitter =
        new FileEmitter(FileEmitterConfig.builder().fileName("test_mcp.json").build());
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {

      DatasetUrn datasetUrn =
          new DatasetUrn(new DataPlatformUrn("hive"), "SampleHiveDataset", FabricType.PROD);
      MetadataChangeProposal ownershipPatch =
          new OwnershipPatchBuilder()
              .urn(datasetUrn)
              .removeOwnershipType(new CorpuserUrn("gdoe"), OwnershipType.TECHNICAL_OWNER)
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

  @Test
  @Ignore
  public void testLocalDataJobInfo() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {
      MetadataChangeProposal jobInfoToPatch =
          new DataJobInfoPatchBuilder()
              .urn(
                  UrnUtils.getUrn(
                      "urn:li:dataJob:(urn:li:dataFlow:(orchestrator,flowId,cluster),jobId)"))
              .setDescription("something")
              .setName("name")
              .setType("type")
              .addCustomProperty("prop1", "propVal1")
              .addCustomProperty("prop2", "propVal2")
              .build();
      Future<MetadataWriteResponse> response = restEmitter.emit(jobInfoToPatch);

      System.out.println(response.get().getResponseContent());

    } catch (IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalDataJobInfoRemove() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {
      MetadataChangeProposal jobInfoToPatch =
          new DataJobInfoPatchBuilder()
              .urn(
                  UrnUtils.getUrn(
                      "urn:li:dataJob:(urn:li:dataFlow:(orchestrator,flowId,cluster),jobId)"))
              .setDescription(null)
              .removeCustomProperty("prop1")
              .removeCustomProperty("prop2")
              .build();
      Future<MetadataWriteResponse> response = restEmitter.emit(jobInfoToPatch);

      System.out.println(response.get().getResponseContent());

    } catch (IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalDatasetProperties() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {

      DatasetUrn datasetUrn =
          new DatasetUrn(new DataPlatformUrn("hive"), "SampleHiveDataset", FabricType.PROD);
      MetadataChangeProposal datasetPropertiesToPatch =
          new DatasetPropertiesPatchBuilder()
              .urn(datasetUrn)
              .setDescription("something")
              .setName("name")
              .addCustomProperty("prop1", "propVal1")
              .addCustomProperty("prop2", "propVal2")
              .build();
      Future<MetadataWriteResponse> response = restEmitter.emit(datasetPropertiesToPatch);

      System.out.println(response.get().getResponseContent());

    } catch (IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalDatasetPropertiesRemove() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {

      DatasetUrn datasetUrn =
          new DatasetUrn(new DataPlatformUrn("hive"), "SampleHiveDataset", FabricType.PROD);
      MetadataChangeProposal datasetPropertiesToPatch =
          new DatasetPropertiesPatchBuilder()
              .urn(datasetUrn)
              .setDescription(null)
              .setName(null)
              .removeCustomProperty("prop1")
              .removeCustomProperty("prop2")
              .build();
      Future<MetadataWriteResponse> response = restEmitter.emit(datasetPropertiesToPatch);

      System.out.println(response.get().getResponseContent());

    } catch (IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalDataFlowInfo() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {
      MetadataChangeProposal flowInfoToPatch =
          new DataFlowInfoPatchBuilder()
              .urn(UrnUtils.getUrn("urn:li:dataFlow:(orchestrator,flowId,cluster)"))
              .setDescription("something")
              .setName("name")
              .setProject("project")
              .addCustomProperty("prop1", "propVal1")
              .addCustomProperty("prop2", "propVal2")
              .build();
      Future<MetadataWriteResponse> response = restEmitter.emit(flowInfoToPatch);

      System.out.println(response.get().getResponseContent());

    } catch (IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalDataFlowInfoRemove() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {
      MetadataChangeProposal flowInfoToPatch =
          new DataFlowInfoPatchBuilder()
              .urn(UrnUtils.getUrn("urn:li:dataFlow:(orchestrator,flowId,cluster)"))
              .setDescription(null)
              .setProject(null)
              .removeCustomProperty("prop1")
              .removeCustomProperty("prop2")
              .build();
      Future<MetadataWriteResponse> response = restEmitter.emit(flowInfoToPatch);

      System.out.println(response.get().getResponseContent());

    } catch (IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalDataJobInputAdd() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {
      MetadataChangeProposal dataJobIOPatch =
          new DataJobInputOutputPatchBuilder()
              .urn(
                  UrnUtils.getUrn(
                      "urn:li:dataJob:(urn:li:dataFlow:(orchestrator,flowId,cluster),jobId)"))
              .addInputDatasetEdge(
                  DatasetUrn.createFromString(
                      "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"))
              .addOutputDatasetEdge(
                  DatasetUrn.createFromString(
                      "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleHiveDataset,PROD)"))
              .addInputDatajobEdge(
                  DataJobUrn.createFromString(
                      "urn:li:dataJob:(urn:li:dataFlow:(orchestrator,flowId,cluster),jobId2)"))
              .addInputDatasetField(
                  UrnUtils.getUrn(
                      "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD),user_id)"))
              .addOutputDatasetField(
                  UrnUtils.getUrn(
                      "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_id)"))
              .build();
      Future<MetadataWriteResponse> response = restEmitter.emit(dataJobIOPatch);

      System.out.println(response.get().getResponseContent());

    } catch (URISyntaxException | IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalDataJobInputRemove() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {
      MetadataChangeProposal dataJobIOPatch =
          new DataJobInputOutputPatchBuilder()
              .urn(
                  UrnUtils.getUrn(
                      "urn:li:dataJob:(urn:li:dataFlow:(orchestrator,flowId,cluster),jobId)"))
              .removeInputDatasetEdge(
                  DatasetUrn.createFromString(
                      "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"))
              .removeOutputDatasetEdge(
                  DatasetUrn.createFromString(
                      "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleHiveDataset,PROD)"))
              .removeInputDatajobEdge(
                  DataJobUrn.createFromString(
                      "urn:li:dataJob:(urn:li:dataFlow:(orchestrator,flowId,cluster),jobId2)"))
              .removeInputDatasetField(
                  UrnUtils.getUrn(
                      "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD),user_id)"))
              .removeOutputDatasetField(
                  UrnUtils.getUrn(
                      "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_id)"))
              .build();
      Future<MetadataWriteResponse> response = restEmitter.emit(dataJobIOPatch);

      System.out.println(response.get().getResponseContent());

    } catch (URISyntaxException | IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalDataJobInputAddEdge() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {
      Edge inputDataset =
          new Edge()
              .setDestinationUrn(
                  DatasetUrn.createFromString(
                      "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"))
              .setCreated(
                  new AuditStamp()
                      .setTime(System.currentTimeMillis())
                      .setActor(UrnUtils.getUrn(UNKNOWN_ACTOR)))
              .setLastModified(
                  new AuditStamp()
                      .setTime(System.currentTimeMillis())
                      .setActor(UrnUtils.getUrn(UNKNOWN_ACTOR)));
      Edge outputDataset =
          new Edge()
              .setDestinationUrn(
                  DatasetUrn.createFromString(
                      "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleHiveDataset,PROD)"))
              .setCreated(
                  new AuditStamp()
                      .setTime(System.currentTimeMillis())
                      .setActor(UrnUtils.getUrn(UNKNOWN_ACTOR)))
              .setLastModified(
                  new AuditStamp()
                      .setTime(System.currentTimeMillis())
                      .setActor(UrnUtils.getUrn(UNKNOWN_ACTOR)));
      Edge inputDataJob =
          new Edge()
              .setDestinationUrn(
                  DataJobUrn.createFromString(
                      "urn:li:dataJob:(urn:li:dataFlow:(orchestrator,flowId,cluster),jobId2)"))
              .setCreated(
                  new AuditStamp()
                      .setTime(System.currentTimeMillis())
                      .setActor(UrnUtils.getUrn(UNKNOWN_ACTOR)))
              .setLastModified(
                  new AuditStamp()
                      .setTime(System.currentTimeMillis())
                      .setActor(UrnUtils.getUrn(UNKNOWN_ACTOR)));
      MetadataChangeProposal dataJobIOPatch =
          new DataJobInputOutputPatchBuilder()
              .urn(
                  UrnUtils.getUrn(
                      "urn:li:dataJob:(urn:li:dataFlow:(orchestrator,flowId,cluster),jobId)"))
              .addEdge(inputDataset, LineageDirection.UPSTREAM)
              .addEdge(outputDataset, LineageDirection.DOWNSTREAM)
              .addEdge(inputDataJob, LineageDirection.UPSTREAM)
              .build();
      Future<MetadataWriteResponse> response = restEmitter.emit(dataJobIOPatch);

      System.out.println(response.get().getResponseContent());

    } catch (URISyntaxException | IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }
}

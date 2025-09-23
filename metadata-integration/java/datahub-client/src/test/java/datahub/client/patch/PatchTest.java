package datahub.client.patch;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Edge;
import com.linkedin.common.FabricType;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayMap;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.form.FormPrompt;
import com.linkedin.form.FormPromptType;
import com.linkedin.form.StructuredPropertyParams;
import com.linkedin.metadata.aspect.patch.builder.ChartInfoPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.DashboardInfoPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.DataFlowInfoPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.DataJobInfoPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.DataJobInputOutputPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.DatasetPropertiesPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.EditableSchemaMetadataPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.FormInfoPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.OwnershipPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.StructuredPropertiesPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.StructuredPropertyDefinitionPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.UpstreamLineagePatchBuilder;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PropertyCardinality;
import com.linkedin.structured.PropertyValue;
import datahub.client.MetadataWriteResponse;
import datahub.client.file.FileEmitter;
import datahub.client.file.FileEmitterConfig;
import datahub.client.rest.RestEmitter;
import datahub.client.rest.RestEmitterConfig;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
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
      DatasetUrn upstreamUrn =
          DatasetUrn.createFromString(
              "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)");
      Urn upstreamSchemaFieldUrn =
          UrnUtils.getUrn(
              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD), foo)");
      Urn downstreamSchemaFieldUrn =
          UrnUtils.getUrn(
              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD), bar)");
      MetadataChangeProposal upstreamPatch =
          new UpstreamLineagePatchBuilder()
              .urn(
                  UrnUtils.getUrn(
                      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"))
              .addUpstream(upstreamUrn, DatasetLineageType.TRANSFORMED)
              .addFineGrainedUpstreamField(
                  upstreamSchemaFieldUrn, null, "TRANSFORM", downstreamSchemaFieldUrn, null)
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
      Urn upstreamSchemaFieldUrn =
          UrnUtils.getUrn(
              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD), foo)");
      Urn downstreamSchemaFieldUrn =
          UrnUtils.getUrn(
              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD), bar)");
      MetadataChangeProposal upstreamPatch =
          new UpstreamLineagePatchBuilder()
              .urn(
                  UrnUtils.getUrn(
                      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"))
              .removeUpstream(
                  DatasetUrn.createFromString(
                      "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"))
              .removeFineGrainedUpstreamField(
                  upstreamSchemaFieldUrn, "TRANSFORM", downstreamSchemaFieldUrn, null)
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

  @Test
  @Ignore
  public void testLocalChartInfoAdd() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {
      MetadataChangeProposal chartInfoPatch =
          new ChartInfoPatchBuilder()
              .urn(UrnUtils.getUrn("urn:li:chart:(dashboardTool,chartId)"))
              .addInputEdge(
                  DatasetUrn.createFromString(
                      "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleHiveDataset,PROD)"))
              .build();
      Future<MetadataWriteResponse> response = restEmitter.emit(chartInfoPatch);

      System.out.println(response.get().getResponseContent());

    } catch (URISyntaxException | IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalChartInfoRemove() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {
      MetadataChangeProposal chartInfoPatch =
          new ChartInfoPatchBuilder()
              .urn(UrnUtils.getUrn("urn:li:chart:(dashboardTool,chartId)"))
              .removeInputEdge(
                  DatasetUrn.createFromString(
                      "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleHiveDataset,PROD)"))
              .build();
      Future<MetadataWriteResponse> response = restEmitter.emit(chartInfoPatch);

      System.out.println(response.get().getResponseContent());

    } catch (URISyntaxException | IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalDashboardInfoAdd() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {
      MetadataChangeProposal dashboardInfoPatch =
          new DashboardInfoPatchBuilder()
              .urn(UrnUtils.getUrn("urn:li:dashboard:(dashboardTool,dashboardId)"))
              .addDatasetEdge(
                  DatasetUrn.createFromString(
                      "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleHiveDataset,PROD)"))
              .addChartEdge(ChartUrn.createFromString("urn:li:chart:(dashboartTool, chartId)"))
              .addDashboard(
                  DashboardUrn.createFromString(
                      "urn:li:dashboard:(dashboardTool, childDashboardId)"))
              .build();
      Future<MetadataWriteResponse> response = restEmitter.emit(dashboardInfoPatch);

      System.out.println(response.get().getResponseContent());

    } catch (URISyntaxException | IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalDashboardInfoRemove() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {
      MetadataChangeProposal dashboardInfoPatch =
          new DashboardInfoPatchBuilder()
              .urn(UrnUtils.getUrn("urn:li:dashboard:(dashboardTool,dashboardId)"))
              .removeDatasetEdge(
                  DatasetUrn.createFromString(
                      "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleHiveDataset,PROD)"))
              .removeChartEdge(ChartUrn.createFromString("urn:li:chart:(dashboardTool, chartId)"))
              .removeDashboard(
                  DashboardUrn.createFromString(
                      "urn:li:dashboard:(dashboardTool, childDashboardId)"))
              .build();
      Future<MetadataWriteResponse> response = restEmitter.emit(dashboardInfoPatch);

      System.out.println(response.get().getResponseContent());

    } catch (URISyntaxException | IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalStructuredPropertyDefinitionAdd() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {
      StringArrayMap typeQualifier = new StringArrayMap();
      typeQualifier.put(
          "allowedTypes",
          new StringArray(
              "urn:li:entityType:datahub.corpuser", "urn:li:entityType:datahub.corpGroup"));
      PropertyValue propertyValue1 = new PropertyValue();
      PrimitivePropertyValue value1 = new PrimitivePropertyValue();
      value1.setString("test value 1");
      propertyValue1.setValue(value1);
      PropertyValue propertyValue2 = new PropertyValue();
      PrimitivePropertyValue value2 = new PrimitivePropertyValue();
      value2.setString("test value 2");
      propertyValue2.setValue(value2);

      MetadataChangeProposal structuredPropertyDefinitionPatch =
          new StructuredPropertyDefinitionPatchBuilder()
              .urn(UrnUtils.getUrn("urn:li:structuredProperty:123456"))
              .setQualifiedName("test.testing.123")
              .setDisplayName("Test Display Name")
              .setValueType("urn:li:dataType:datahub.urn")
              .setTypeQualifier(typeQualifier)
              .addAllowedValue(propertyValue1)
              .addAllowedValue(propertyValue2)
              .setCardinality(PropertyCardinality.MULTIPLE)
              .addEntityType("urn:li:entityType:datahub.dataFlow")
              .setDescription("test description")
              .setImmutable(true)
              .build();

      Future<MetadataWriteResponse> response = restEmitter.emit(structuredPropertyDefinitionPatch);

      System.out.println(response.get().getResponseContent());

    } catch (IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalFormInfoAdd() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {
      FormPrompt newPrompt =
          new FormPrompt()
              .setId("1234")
              .setTitle("First Prompt")
              .setType(FormPromptType.STRUCTURED_PROPERTY)
              .setRequired(true)
              .setStructuredPropertyParams(
                  new StructuredPropertyParams()
                      .setUrn(UrnUtils.getUrn("urn:li:structuredProperty:property1")));
      FormPrompt newPrompt2 =
          new FormPrompt()
              .setId("abcd")
              .setTitle("Second Prompt")
              .setType(FormPromptType.FIELDS_STRUCTURED_PROPERTY)
              .setRequired(false)
              .setStructuredPropertyParams(
                  new StructuredPropertyParams()
                      .setUrn(UrnUtils.getUrn("urn:li:structuredProperty:property1")));

      MetadataChangeProposal formInfoPatch =
          new FormInfoPatchBuilder()
              .urn(UrnUtils.getUrn("urn:li:form:123456"))
              .addPrompts(List.of(newPrompt, newPrompt2))
              .setName("Metadata Initiative 2024 (edited)")
              .setDescription("Edited description")
              .setOwnershipForm(true)
              .addAssignedUser("urn:li:corpuser:admin")
              .addAssignedGroup("urn:li:corpGroup:jdoe")
              .build();
      Future<MetadataWriteResponse> response = restEmitter.emit(formInfoPatch);

      System.out.println(response.get().getResponseContent());

    } catch (IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalStructuredPropertiesUpdate() {
    try {
      MetadataChangeProposal mcp =
          new StructuredPropertiesPatchBuilder()
              .urn(
                  UrnUtils.getUrn(
                      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)"))
              .setNumberProperty(
                  UrnUtils.getUrn(
                      "urn:li:structuredProperty:io.acryl.dataManagement.replicationSLA"),
                  3456)
              .build();

      String token = "";
      RestEmitter emitter = RestEmitter.create(b -> b.server("http://localhost:8080").token(token));
      Future<MetadataWriteResponse> response = emitter.emit(mcp, null);
      System.out.println(response.get().getResponseContent());

    } catch (IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }

  @Test
  @Ignore
  public void testLocalFormInfoRemove() {
    RestEmitter restEmitter = new RestEmitter(RestEmitterConfig.builder().build());
    try {
      MetadataChangeProposal formInfoPatch =
          new FormInfoPatchBuilder()
              .urn(UrnUtils.getUrn("urn:li:form:123456"))
              .removePrompts(List.of("1234", "abcd"))
              .setName("Metadata Initiative 2024 (edited - again)")
              .setDescription(null)
              .removeAssignedUser("urn:li:corpuser:admin")
              .removeAssignedGroup("urn:li:corpGroup:jdoe")
              .build();
      Future<MetadataWriteResponse> response = restEmitter.emit(formInfoPatch);

      System.out.println(response.get().getResponseContent());

    } catch (IOException | ExecutionException | InterruptedException e) {
      System.out.println(Arrays.asList(e.getStackTrace()));
    }
  }
}

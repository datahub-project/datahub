package datahub.client.file;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.mxe.MetadataChangeProposal;

import datahub.client.Callback;
import datahub.client.MetadataWriteResponse;
import datahub.event.MetadataChangeProposalWrapper;

public class FileEmitterTest {
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final JacksonDataTemplateCodec dataTemplateCodec = new JacksonDataTemplateCodec(objectMapper.getFactory());

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testFileEmitter() throws IOException {

    InputStream goldenFileStream = ClassLoader.getSystemResourceAsStream("golden_files/mcps_golden.json");

    String tempRoot = tempFolder.getRoot().toString();
    String outputFile = tempRoot + "/test.json";
    FileEmitter emitter = new FileEmitter(FileEmitterConfig.builder().fileName(outputFile).build());
    for (MetadataChangeProposal mcp : this.getMCPs(goldenFileStream)) {
      emitter.emit(mcp);
    }
    emitter.close();
    goldenFileStream = ClassLoader.getSystemResourceAsStream("golden_files/mcps_golden.json");
    this.assertEqualJsonFile(goldenFileStream, outputFile);

  }

  private void assertEqualJsonFile(InputStream file1, String file2) throws StreamReadException, DatabindException,
      IOException {
    TypeReference<List<Map<String, Object>>> typeRef = new TypeReference<List<Map<String, Object>>>() {
    };
    List<Map<String, Object>> map1 = this.objectMapper.readValue(file1, typeRef);
    File f2 = new File(file2);
    List<Map<String, Object>> map2 = this.objectMapper.readValue(f2, typeRef);
    Assert.assertEquals(map1, map2);
  }

  private List<MetadataChangeProposal> getMCPs(InputStream fileStream) throws StreamReadException, DatabindException,
                                                                            IOException {
    ArrayList<MetadataChangeProposal> mcps = new ArrayList<MetadataChangeProposal>();
    TypeReference<Map<String, Object>[]> typeRef = new TypeReference<Map<String, Object>[]>() {
    };
    Map<String, Object>[] maps = this.objectMapper.readValue(fileStream, typeRef);
    for (Map<String, Object> map : maps) {
      String json = objectMapper.writeValueAsString(map);
      DataMap data = dataTemplateCodec.stringToMap(json);
      mcps.add(new MetadataChangeProposal(data));
    }
    return mcps;
  }

  @Test
  public void testSuccessCallback() throws Exception {

    String tempRoot = tempFolder.getRoot().toString();
    String outputFile = tempRoot + "/testCallBack.json";
    FileEmitter emitter = new FileEmitter(FileEmitterConfig.builder().fileName(outputFile).build());
    MetadataChangeProposalWrapper<?> mcpw = getMetadataChangeProposalWrapper("Test Dataset", "urn:li:dataset:foo");
    AtomicReference<MetadataWriteResponse> callbackResponse = new AtomicReference<>();
    Future<MetadataWriteResponse> future = emitter.emit(mcpw, new Callback() {
      @Override
      public void onCompletion(MetadataWriteResponse response) {
        callbackResponse.set(response);
        Assert.assertTrue(response.isSuccess());
      }

      @Override
      public void onFailure(Throwable exception) {
        Assert.fail("Should not be called");
      }
    });

    Assert.assertEquals(callbackResponse.get(), future.get());
  }

  @Test
  public void testFailCallback() throws Exception {

    String tempRoot = tempFolder.getRoot().toString();
    String outputFile = tempRoot + "/testCallBack.json";
    FileEmitter emitter = new FileEmitter(FileEmitterConfig.builder().fileName(outputFile).build());
    emitter.close();
    MetadataChangeProposalWrapper<?> mcpw = getMetadataChangeProposalWrapper("Test Dataset", "urn:li:dataset:foo");
    Future<MetadataWriteResponse> future = emitter.emit(mcpw, new Callback() {
      @Override
      public void onCompletion(MetadataWriteResponse response) {

        Assert.fail("Should not be called");
      }

      @Override
      public void onFailure(Throwable exception) {

      }
    });

    Assert.assertFalse(future.get().isSuccess());

  }

  private MetadataChangeProposalWrapper<?> getMetadataChangeProposalWrapper(String description, String entityUrn) {
    return MetadataChangeProposalWrapper.builder()
        .entityType("dataset")
        .entityUrn(entityUrn)
        .upsert()
        .aspect(new DatasetProperties().setDescription(description))
        .build();
  }

}

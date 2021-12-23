package datahub.client;

import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class RestEmitterTest {

  @Mock
  HttpClient mockClient;

  @Captor
  ArgumentCaptor<HttpPost> postArgumentCaptor;

  @Test
  public void testPost() throws URISyntaxException, IOException {

    HttpClientFactory mockHttpClientFactory = new HttpClientFactory() {
      @Override
      public HttpClient getHttpClient() {
        return mockClient;
      }
    };

    RestEmitter emitter = new RestEmitter("http://localhost:8080", 30, 30, null, mockHttpClientFactory);
    MetadataChangeProposalWrapper mcp =
        new MetadataChangeProposalWrapper.MetadataChangeProposalWrapperBuilder().entityType("dataset")
            .aspectName("datasetProperties")
            .changeType(ChangeType.UPSERT)
            .aspect(new DatasetProperties().setDescription("Test Dataset"))
            .entityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,foo.bar,PROD)"))
            .build();
    emitter.emit(mcp);
    Mockito.verify(mockClient).execute(postArgumentCaptor.capture());
    HttpPost testPost = postArgumentCaptor.getValue();
    Assert.assertEquals("2.0.0", testPost.getFirstHeader("X-RestLi-Protocol-Version").getValue());
    InputStream is = testPost.getEntity().getContent();
    byte[] contentBytes = new byte[(int) testPost.getEntity().getContentLength()];
    is.read(contentBytes);
    String contentString = new String(contentBytes, Charset.forName("UTF-8"));
    String expectedContent =
        "{\"proposal\":{\"aspectName\":\"datasetProperties\","
            + "\"entityUrn\":\"urn:li:dataset:(urn:li:dataPlatform:hive,foo.bar,PROD)\","
            + "\"entityType\":\"dataset\",\"changeType\":\"UPSERT\",\"aspect\":{\"contentType\":\"application/json\""
            + ",\"value\":\"{\\\"description\\\":\\\"Test Dataset\\\"}\"}}}";
    Assert.assertEquals(expectedContent, contentString);
  }
}
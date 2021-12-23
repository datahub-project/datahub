package datahub.client;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.ByteString;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;


@Slf4j
@RequiredArgsConstructor
public class RestEmitter {

  private static final int DEFAULT_CONNECT_TIMEOUT_SEC = 30;
  private static final int DEFAULT_READ_TIMEOUT_SEC = 30;
  private static final String DEFAULT_AUTH_TOKEN = null;
  private static final HttpClientFactory DEFAULT_HTTP_CLIENT_FACTORY = new DefaultHttpClientFactory();


  @Getter
  private final String gmsUrl;

  @Getter
  private final int connectTimeoutSec;

  @Getter
  private final int readTimeoutSec;

  @Getter
  private final String token;

  @Getter
  private final HttpClientFactory httpClientFactory;

  private final String ingestProposalUrl;
  private final String configUrl;

  private final ObjectMapper objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
  private final JacksonDataTemplateCodec dataTemplateCodec = new JacksonDataTemplateCodec(objectMapper.getFactory());
  private final HttpClient httpClient;

  public RestEmitter(String serverAddr, int connectTimeoutSec, int readTimeoutSec, String token, HttpClientFactory httpClientFactory) {
    this.gmsUrl = serverAddr;
    this.connectTimeoutSec = connectTimeoutSec;
    this.readTimeoutSec = readTimeoutSec;
    this.token = token;
    this.httpClientFactory = httpClientFactory;
    this.httpClient = this.httpClientFactory.getHttpClient();
    this.ingestProposalUrl = this.gmsUrl + "/aspects?action=ingestProposal";
    this.configUrl = this.gmsUrl + "/config";
  }

  public void emit(MetadataChangeProposalWrapper mcpw) throws IOException {
    String serializedAspect = dataTemplateCodec.dataTemplateToString(mcpw.getAspect());
    MetadataChangeProposal mcp = new MetadataChangeProposal().setEntityType(mcpw.getEntityType())
        .setAspectName(mcpw.getAspectName())
        .setEntityUrn(mcpw.getEntityUrn())
        .setChangeType(mcpw.getChangeType())
        .setAspect(new GenericAspect().setContentType("application/json")
            .setValue(ByteString.unsafeWrap(serializedAspect.getBytes(StandardCharsets.UTF_8))));
    emit(mcp);
  }

  public void emit(MetadataChangeProposal mcp) throws IOException {
    DataMap map = new DataMap();
    map.put("proposal", mcp.data());
    String serializedMCP = dataTemplateCodec.mapToString(map);
    log.debug("Emitting payload: " + serializedMCP + "\n to URL " + this.ingestProposalUrl);
    this.postGeneric(this.ingestProposalUrl, serializedMCP);
  }

  private boolean postGeneric(String urlStr, String payloadJson) throws IOException {
    HttpPost httpPost = new HttpPost(urlStr);
    httpPost.setHeader("Content-Type", "application/json");
    httpPost.setHeader("X-RestLi-Protocol-Version", "2.0.0");
    httpPost.setHeader("Accept", "application/json");
    if (token != null) {
      httpPost.setHeader("Authorization", "Bearer " + token);
    }
    httpPost.setEntity(new StringEntity(payloadJson));
    HttpResponse response = httpClient.execute(httpPost);
    return (response != null && response.getStatusLine() != null && response.getStatusLine().getStatusCode() == 200);
  }

  private boolean getGeneric(String urlStr) throws IOException {
    HttpGet httpGet = new HttpGet(urlStr);
    httpGet.setHeader("Content-Type", "application/json");
    httpGet.setHeader("X-RestLi-Protocol-Version", "2.0.0");
    httpGet.setHeader("Accept", "application/json");
    HttpResponse response = this.httpClient.execute(httpGet);
    return (response.getStatusLine().getStatusCode() == 200);
  }

  public boolean testConnection() throws IOException {
    return this.getGeneric(this.configUrl);
  }

  public static RestEmitter create(String gmsUrl) {
    return new RestEmitter(gmsUrl, DEFAULT_CONNECT_TIMEOUT_SEC, DEFAULT_READ_TIMEOUT_SEC, null, DEFAULT_HTTP_CLIENT_FACTORY);
  }

  public static RestEmitter create(String gmsUrl, int connectTimeoutSec, int readTimeoutSec) {
    return new RestEmitter(gmsUrl, connectTimeoutSec, readTimeoutSec, null, DEFAULT_HTTP_CLIENT_FACTORY);
  }

  public static RestEmitter create(String gmsUrl, String token) {
    return new RestEmitter(gmsUrl, DEFAULT_CONNECT_TIMEOUT_SEC, DEFAULT_READ_TIMEOUT_SEC, token, DEFAULT_HTTP_CLIENT_FACTORY);
  }

  public static RestEmitter create(String gmsUrl, int connectTimeoutSec, int readTimeoutSec, String token) {
    return new RestEmitter(gmsUrl, connectTimeoutSec, readTimeoutSec, token, DEFAULT_HTTP_CLIENT_FACTORY);
  }

  public static RestEmitter create(String gmsUrl, HttpClientFactory httpClientFactory) {
    return new RestEmitter(gmsUrl, DEFAULT_CONNECT_TIMEOUT_SEC, DEFAULT_READ_TIMEOUT_SEC, DEFAULT_AUTH_TOKEN, DEFAULT_HTTP_CLIENT_FACTORY);
  }
}
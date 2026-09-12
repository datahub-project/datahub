package com.linkedin.gms.factory.kafka;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Collections;
import javax.security.auth.callback.Callback;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.msk.auth.iam.internals.AWSCredentialsCallback;

public class DataHubMskIamClientCallbackHandlerTest {

  private StaticCredentialsProvider credentialsProvider;

  @AfterMethod
  public void tearDown() {
    DataHubMskIamClientCallbackHandler.resetIfInstalled(credentialsProvider);
  }

  @Test
  public void resolvesCredentialsFromProcessProvider() throws Exception {
    credentialsProvider =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("id", "secret"));
    DataHubMskIamClientCallbackHandler.installSharedCredentials(credentialsProvider);
    DataHubMskIamClientCallbackHandler handler = new DataHubMskIamClientCallbackHandler();
    handler.configure(Collections.emptyMap(), "AWS_MSK_IAM", Collections.emptyList());
    AWSCredentialsCallback callback = new AWSCredentialsCallback();

    handler.handle(new Callback[] {callback});
    handler.close();

    assertTrue(callback.isSuccessful());
    assertEquals(callback.getAwsCredentials().accessKeyId(), "id");
  }

  @Test
  public void reportsMissingProcessProvider() throws Exception {
    DataHubMskIamClientCallbackHandler handler = new DataHubMskIamClientCallbackHandler();
    AWSCredentialsCallback callback = new AWSCredentialsCallback();

    handler.handle(new Callback[] {callback});

    assertFalse(callback.isSuccessful());
    assertTrue(callback.getLoadingException() instanceof IllegalStateException);
  }
}

package com.linkedin.gms.factory.kafka;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

public class KafkaMskIamAuthTest {

  private StaticCredentialsProvider credentialsProvider;

  @BeforeMethod
  public void setUp() {
    credentialsProvider =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("id", "secret"));
    DataHubMskIamClientCallbackHandler.installSharedCredentials(credentialsProvider);
  }

  @AfterMethod
  public void tearDown() {
    DataHubMskIamClientCallbackHandler.resetIfInstalled(credentialsProvider);
  }

  @Test
  public void disablesAwsDebugCredsOnIamLoginModuleJaas() {
    Map<String, Object> props = new HashMap<>();
    props.put(
        "sasl.jaas.config",
        "software.amazon.msk.auth.iam.IAMLoginModule required awsDebugCreds=true awsRoleArn=\"arn:aws:iam::1:role/r\";");
    KafkaMskIamAuth.configure(props);
    assertEquals(
        props.get("sasl.jaas.config"),
        "software.amazon.msk.auth.iam.IAMLoginModule required awsDebugCreds=false awsRoleArn=\"arn:aws:iam::1:role/r\";");
  }

  @Test
  public void disablesAwsDebugCredsWhenMechanismIsAwsMskIam() {
    Map<String, Object> props = new HashMap<>();
    props.put("sasl.mechanism", "AWS_MSK_IAM");
    props.put("sasl.jaas.config", "some.OtherModule required awsDebugCreds=true;");
    KafkaMskIamAuth.configure(props);
    assertEquals(props.get("sasl.jaas.config"), "some.OtherModule required awsDebugCreds=false;");
  }

  @Test
  public void disablesQuotedAwsDebugCreds() {
    Map<String, Object> props = new HashMap<>();
    props.put(
        "sasl.jaas.config",
        "software.amazon.msk.auth.iam.IAMLoginModule required awsDebugCreds=\"true\";");
    KafkaMskIamAuth.configure(props);
    assertEquals(
        props.get("sasl.jaas.config"),
        "software.amazon.msk.auth.iam.IAMLoginModule required awsDebugCreds=false;");
  }

  @Test
  public void disablesAwsDebugCredsWithWhitespaceAroundEquals() {
    Map<String, Object> props = new HashMap<>();
    props.put(
        "sasl.jaas.config",
        "software.amazon.msk.auth.iam.IAMLoginModule required awsDebugCreds = true;");
    KafkaMskIamAuth.configure(props);
    assertEquals(
        props.get("sasl.jaas.config"),
        "software.amazon.msk.auth.iam.IAMLoginModule required awsDebugCreds=false;");
  }

  @Test
  public void leavesNonMskJaasUnchanged() {
    Map<String, Object> props = new HashMap<>();
    props.put(
        "sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required;");
    KafkaMskIamAuth.configure(props);
    assertFalse(props.get("sasl.jaas.config").toString().contains("awsDebugCreds"));
  }

  @Test
  public void replacesDefaultMskCallbackForFallbackCredentials() {
    Map<String, Object> props = new HashMap<>();
    props.put("sasl.mechanism", "AWS_MSK_IAM");
    props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
    props.put(
        "sasl.client.callback.handler.class",
        "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

    KafkaMskIamAuth.configure(props);

    assertEquals(
        props.get("sasl.client.callback.handler.class"),
        DataHubMskIamClientCallbackHandler.class.getName());
  }

  @Test
  public void preservesExplicitRoleCredentialConfiguration() {
    Map<String, Object> props = new HashMap<>();
    props.put("sasl.mechanism", "AWS_MSK_IAM");
    props.put(
        "sasl.jaas.config",
        "software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn=\"arn:aws:iam::1:role/r\";");
    props.put(
        "sasl.client.callback.handler.class",
        "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

    KafkaMskIamAuth.configure(props);

    assertEquals(
        props.get("sasl.client.callback.handler.class"),
        "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
  }

  @Test
  public void preservesCustomCallbackHandler() {
    Map<String, Object> props = new HashMap<>();
    props.put("sasl.mechanism", "AWS_MSK_IAM");
    props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
    props.put("sasl.client.callback.handler.class", "example.CustomCallbackHandler");

    KafkaMskIamAuth.configure(props);

    assertEquals(props.get("sasl.client.callback.handler.class"), "example.CustomCallbackHandler");
  }
}

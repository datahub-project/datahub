package com.linkedin.gms.factory.kafka;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class KafkaMskIamAuthTest {

  @Test
  public void disablesAwsDebugCredsOnIamLoginModuleJaas() {
    Map<String, Object> props = new HashMap<>();
    props.put(
        "sasl.jaas.config",
        "software.amazon.msk.auth.iam.IAMLoginModule required awsDebugCreds=true awsRoleArn=\"arn:aws:iam::1:role/r\";");
    KafkaMskIamAuth.disableDebugCallerIdentity(props);
    assertEquals(
        props.get("sasl.jaas.config"),
        "software.amazon.msk.auth.iam.IAMLoginModule required awsDebugCreds=false awsRoleArn=\"arn:aws:iam::1:role/r\";");
  }

  @Test
  public void disablesAwsDebugCredsWhenMechanismIsAwsMskIam() {
    Map<String, Object> props = new HashMap<>();
    props.put("sasl.mechanism", "AWS_MSK_IAM");
    props.put("sasl.jaas.config", "some.OtherModule required awsDebugCreds=true;");
    KafkaMskIamAuth.disableDebugCallerIdentity(props);
    assertEquals(props.get("sasl.jaas.config"), "some.OtherModule required awsDebugCreds=false;");
  }

  @Test
  public void leavesNonMskJaasUnchanged() {
    Map<String, Object> props = new HashMap<>();
    props.put(
        "sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required;");
    KafkaMskIamAuth.disableDebugCallerIdentity(props);
    assertFalse(props.get("sasl.jaas.config").toString().contains("awsDebugCreds"));
  }
}

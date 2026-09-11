package com.linkedin.gms.factory.kafka;

import java.util.Map;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

/**
 * aws-msk-iam-auth builds its own credential chain. {@code awsDebugCreds=true} constructs an
 * unclosed {@code StsClient} for {@code GetCallerIdentity}. Force that option off when we see MSK
 * IAM JAAS; the library does not accept an injected {@code AwsCredentialsProvider}.
 */
@Slf4j
final class KafkaMskIamAuth {

  private static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
  private static final String SASL_MECHANISM = "sasl.mechanism";
  private static final String AWS_MSK_IAM = "AWS_MSK_IAM";
  private static final String IAM_LOGIN_MODULE = "IAMLoginModule";
  private static final Pattern AWS_DEBUG_CREDS_TRUE =
      Pattern.compile("awsDebugCreds\\s*=\\s*\"?true\"?", Pattern.CASE_INSENSITIVE);

  private KafkaMskIamAuth() {}

  static void disableDebugCallerIdentity(Map<String, Object> kafkaProperties) {
    if (kafkaProperties == null || !isMskIam(kafkaProperties)) {
      return;
    }
    Object jaas = kafkaProperties.get(SASL_JAAS_CONFIG);
    if (!(jaas instanceof String jaasConfig) || jaasConfig.isBlank()) {
      return;
    }
    if (!AWS_DEBUG_CREDS_TRUE.matcher(jaasConfig).find()) {
      return;
    }
    kafkaProperties.put(
        SASL_JAAS_CONFIG,
        AWS_DEBUG_CREDS_TRUE.matcher(jaasConfig).replaceAll("awsDebugCreds=false"));
    log.warn(
        "Disabled awsDebugCreds on MSK IAM JAAS config (GetCallerIdentity StsClient is never closed)");
  }

  private static boolean isMskIam(Map<String, Object> kafkaProperties) {
    Object mechanism = kafkaProperties.get(SASL_MECHANISM);
    if (mechanism != null && AWS_MSK_IAM.equalsIgnoreCase(mechanism.toString().trim())) {
      return true;
    }
    Object jaas = kafkaProperties.get(SASL_JAAS_CONFIG);
    return jaas != null && jaas.toString().contains(IAM_LOGIN_MODULE);
  }
}

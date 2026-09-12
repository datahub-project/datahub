package com.linkedin.gms.factory.kafka;

import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Applies resource-safe configuration for the aws-msk-iam-auth library. */
@Slf4j
public final class KafkaMskIamAuth {

  private static final String AWS_MSK_CALLBACK_HANDLER =
      "software.amazon.msk.auth.iam.IAMClientCallbackHandler";
  private static final String CALLBACK_HANDLER_CONFIG = "sasl.client.callback.handler.class";
  private static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
  private static final String SASL_MECHANISM = "sasl.mechanism";
  private static final String AWS_MSK_IAM = "AWS_MSK_IAM";
  private static final String IAM_LOGIN_MODULE = "IAMLoginModule";
  private static final Pattern EXPLICIT_CREDENTIAL_OPTIONS =
      Pattern.compile(
          "aws(?:ProfileName|RoleArn|RoleAccessKeyId|RoleSecretAccessKey|RoleSessionToken|RoleExternalId)\\s*=",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern AWS_DEBUG_CREDS_TRUE =
      Pattern.compile(
          "(?<!\\S)awsDebugCreds\\s*=\\s*\"?true\"?(?=\\s|;|$)", Pattern.CASE_INSENSITIVE);

  private KafkaMskIamAuth() {}

  static void configure(Map<String, Object> kafkaProperties) {
    if (kafkaProperties == null || !isMskIam(kafkaProperties)) {
      return;
    }
    installSharedCredentialsCallback(kafkaProperties);
    disableDebugCallerIdentity(kafkaProperties);
  }

  private static void installSharedCredentialsCallback(Map<String, Object> kafkaProperties) {
    Object jaas = kafkaProperties.get(SASL_JAAS_CONFIG);
    if (jaas != null && EXPLICIT_CREDENTIAL_OPTIONS.matcher(jaas.toString()).find()) {
      return;
    }

    Object configuredHandler = kafkaProperties.get(CALLBACK_HANDLER_CONFIG);
    if (configuredHandler != null && !isDefaultMskCallback(configuredHandler)) {
      return;
    }
    if (!DataHubMskIamClientCallbackHandler.isSharedCredentialsInstalled()) {
      throw new IllegalStateException(
          "MSK IAM is configured but shared AWS credentials are unavailable");
    }

    kafkaProperties.put(
        CALLBACK_HANDLER_CONFIG, DataHubMskIamClientCallbackHandler.class.getName());
  }

  private static boolean isDefaultMskCallback(Object configuredHandler) {
    if (configuredHandler instanceof Class<?> handlerClass) {
      return AWS_MSK_CALLBACK_HANDLER.equals(handlerClass.getName());
    }
    return AWS_MSK_CALLBACK_HANDLER.equals(configuredHandler.toString().trim());
  }

  private static void disableDebugCallerIdentity(Map<String, Object> kafkaProperties) {
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

  public static boolean isMskIam(@Nullable Map<String, Object> kafkaProperties) {
    if (kafkaProperties == null) {
      return false;
    }
    Object mechanism = kafkaProperties.get(SASL_MECHANISM);
    if (mechanism != null && AWS_MSK_IAM.equalsIgnoreCase(mechanism.toString().trim())) {
      return true;
    }
    Object jaas = kafkaProperties.get(SASL_JAAS_CONFIG);
    return jaas != null && jaas.toString().contains(IAM_LOGIN_MODULE);
  }
}

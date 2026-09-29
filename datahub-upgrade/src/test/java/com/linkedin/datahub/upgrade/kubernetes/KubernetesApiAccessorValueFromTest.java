package com.linkedin.datahub.upgrade.kubernetes;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.client.dsl.internal.PatchUtils;
import io.fabric8.kubernetes.client.utils.KubernetesSerialization;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import org.testng.annotations.Test;

/**
 * Regression coverage: setDeploymentEnv must preserve {@code valueFrom}-sourced env vars (Secret /
 * ConfigMap / FieldRef) on the GMS deployment when scale-down only intends to overwrite literal
 * keys (PRE_PROCESS_HOOKS_UI_ENABLED, MCE_CONSUMER_ENABLED, …).
 *
 * <p>The bug: replacing the env list with {@code new ArrayList<>(byName.values())} reorders entries
 * non-deterministically (HashMap iteration order). fabric8's JSON Patch generator is index-based,
 * so a reordered array becomes a series of in-place field rewrites — including {@code remove
 * /env/N/valueFrom} — that the API server applies, destroying the secret binding at every shifted
 * position. With small env lists the diff happens to use {@code add}+{@code remove}
 * (non-destructive); with realistic GMS shape (~30 vars) the destructive pattern dominates.
 *
 * <p>Tests are layered so a regression points at the responsible stage:
 *
 * <ol>
 *   <li>{@link #cloneRoundTripPreservesValueFrom()} — fabric8's deep-clone (Jackson round-trip).
 *   <li>{@link #operatorPreservesValueFromOnUnrelatedKeys()} — the UnaryOperator's in-memory
 *       effect.
 *   <li>{@link #jsonPatchDiffDoesNotTouchUnrelatedValueFromEnv()} — small-list diff.
 *   <li>{@link #jsonPatchDiffWithRealisticGmsEnv()} — the realistic shape that exposes the bug.
 * </ol>
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class KubernetesApiAccessorValueFromTest {

  private static final String NAMESPACE = "datahub-ns";
  private static final String GMS_DEPLOYMENT = "datahub-gms";
  private static final String SSL_PASSWORD_ENV = "SPRING_KAFKA_PROPERTIES_SSL_KEYSTORE_PASSWORD";
  private static final String UI_HOOKS_ENV = "PRE_PROCESS_HOOKS_UI_ENABLED";

  /** Mirrors what helm renders for GMS: SSL keystore password sourced via secretKeyRef. */
  private static EnvVar valueFromSecret(String name, String secret, String key) {
    return new EnvVarBuilder()
        .withName(name)
        .withNewValueFrom()
        .withNewSecretKeyRef()
        .withName(secret)
        .withKey(key)
        .endSecretKeyRef()
        .endValueFrom()
        .build();
  }

  private static Deployment buildGmsDeployment() {
    EnvVar sslKeystorePassword =
        valueFromSecret(SSL_PASSWORD_ENV, "ssl-config", "keystore_password");
    EnvVar uiHooks = new EnvVar(UI_HOOKS_ENV, "true", null);
    return new DeploymentBuilder()
        .withNewMetadata()
        .withName(GMS_DEPLOYMENT)
        .withNamespace(NAMESPACE)
        .endMetadata()
        .withNewSpec()
        .withReplicas(1)
        .withNewTemplate()
        .withNewSpec()
        .addNewContainer()
        .withName("datahub-gms")
        .withEnv(sslKeystorePassword, uiHooks)
        .endContainer()
        .endSpec()
        .endTemplate()
        .endSpec()
        .build();
  }

  private static EnvVar findEnv(Deployment d, String name) {
    return d.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream()
        .filter(e -> name.equals(e.getName()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("env var not found: " + name));
  }

  /**
   * Captures the UnaryOperator that setDeploymentEnv installs via .edit(), so we can apply it to a
   * clone of the deployment ourselves and observe its effect on env vars.
   */
  private static UnaryOperator<Deployment> captureOperator(Deployment original) {
    AtomicReference<UnaryOperator<Deployment>> captured = new AtomicReference<>();
    KubernetesClient client = mock(KubernetesClient.class);
    AppsAPIGroupDSL apps = mock(AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    RollableScalableResource<Deployment> resource = mock(RollableScalableResource.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.withName(GMS_DEPLOYMENT)).thenReturn(resource);
    when(resource.get()).thenReturn(original);
    when(resource.edit(any(UnaryOperator.class)))
        .thenAnswer(
            inv -> {
              captured.set(inv.getArgument(0));
              return original;
            });

    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    accessor.setDeploymentEnv(GMS_DEPLOYMENT, NAMESPACE, Map.of(UI_HOOKS_ENV, "false"));
    UnaryOperator<Deployment> op = captured.get();
    assertNotNull(op, "setDeploymentEnv did not invoke .edit(UnaryOperator)");
    return op;
  }

  /**
   * STAGE 1: fabric8's HasMetadataOperation.edit() deep-clones the resource via
   * KubernetesSerialization.clone() before passing it to the operator. If the round-trip strips
   * valueFrom, every downstream stage operates on a corrupted resource.
   */
  @Test
  public void cloneRoundTripPreservesValueFrom() {
    Deployment original = buildGmsDeployment();
    KubernetesSerialization serialization = new KubernetesSerialization();

    Deployment cloned = serialization.clone(original);

    EnvVar ssl = findEnv(cloned, SSL_PASSWORD_ENV);
    assertNotNull(
        ssl.getValueFrom(),
        "fabric8 KubernetesSerialization.clone() dropped valueFrom — Jackson round-trip is the bug");
    assertNotNull(ssl.getValueFrom().getSecretKeyRef());
    assertEquals(ssl.getValueFrom().getSecretKeyRef().getName(), "ssl-config");
    assertEquals(ssl.getValueFrom().getSecretKeyRef().getKey(), "keystore_password");
    assertNull(
        ssl.getValue(),
        "Cloned EnvVar has a literal value populated alongside valueFrom — round-trip is leaking value=\"\"");
  }

  /**
   * STAGE 2: After clone, fabric8 applies the user's UnaryOperator. Verify our operator does not
   * touch the SSL EnvVar's valueFrom or set an empty value on it.
   */
  @Test
  public void operatorPreservesValueFromOnUnrelatedKeys() {
    Deployment original = buildGmsDeployment();
    UnaryOperator<Deployment> op = captureOperator(original);

    KubernetesSerialization serialization = new KubernetesSerialization();
    Deployment cloned = serialization.clone(original);
    Deployment modified = op.apply(cloned);

    EnvVar ssl = findEnv(modified, SSL_PASSWORD_ENV);
    assertNotNull(
        ssl.getValueFrom(),
        "setDeploymentEnv UnaryOperator stripped valueFrom from an unrelated EnvVar");
    assertNull(
        ssl.getValue(),
        "setDeploymentEnv UnaryOperator added a literal empty value to an EnvVar that uses valueFrom");

    EnvVar ui = findEnv(modified, UI_HOOKS_ENV);
    assertEquals(ui.getValue(), "false", "setDeploymentEnv did not update the targeted key");
    assertNull(ui.getValueFrom());

    List<EnvVar> envList =
        modified.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
    assertEquals(envList.size(), 2, "operator should not have added or removed entries");
  }

  /**
   * Mirrors the realistic GMS env list that helm renders in AWS (cloud-aws.yaml) — many literal env
   * vars plus several valueFrom-sourced ones (SPRING_KAFKA_PROPERTIES_SSL_*_PASSWORD,
   * DATAHUB_TOKEN_SERVICE_*, DATAHUB_SYSTEM_CLIENT_SECRET, SECRET_SERVICE_ENCRYPTION_KEY,
   * SENTRY_DSN). The deploymentEnvUpdates flow targets only the 5 *_CONSUMER_ENABLED /
   * PRE_PROCESS_HOOKS_UI_ENABLED keys.
   */
  private static Deployment buildRealisticGmsDeployment() {
    EnvVarBuilder b = new EnvVarBuilder();
    List<EnvVar> envs =
        List.of(
            new EnvVar("DATAHUB_GMS_HOST", "datahub-gms", null),
            new EnvVar("DATAHUB_GMS_PORT", "8080", null),
            new EnvVar("EBEAN_DATASOURCE_HOST", "mysql:3306", null),
            new EnvVar("EBEAN_DATASOURCE_URL", "jdbc:mysql://mysql:3306/datahub", null),
            new EnvVar("EBEAN_DATASOURCE_DRIVER", "com.mysql.cj.jdbc.Driver", null),
            new EnvVar("KAFKA_BOOTSTRAP_SERVER", "broker:9092", null),
            new EnvVar("SCHEMA_REGISTRY_TYPE", "INTERNAL", null),
            new EnvVar("ELASTICSEARCH_HOST", "elasticsearch", null),
            new EnvVar("ELASTICSEARCH_PORT", "9200", null),
            new EnvVar("GRAPH_SERVICE_IMPL", "elasticsearch", null),
            new EnvVar("ENTITY_SERVICE_IMPL", "ebean", null),
            new EnvVar("PRE_PROCESS_HOOKS_UI_ENABLED", "true", null),
            new EnvVar("MCE_CONSUMER_ENABLED", "true", null),
            new EnvVar("MAE_CONSUMER_ENABLED", "true", null),
            new EnvVar("PE_CONSUMER_ENABLED", "true", null),
            new EnvVar("MCP_CONSUMER_ENABLED", "true", null),
            new EnvVar("METADATA_SERVICE_AUTH_ENABLED", "true", null),
            new EnvVar("KAFKA_PROPERTIES_SECURITY_PROTOCOL", "SSL", null),
            new EnvVar(
                "SPRING_KAFKA_PROPERTIES_SSL_KEYSTORE_LOCATION", "/mnt/certs/keystore", null),
            new EnvVar(
                "SPRING_KAFKA_PROPERTIES_SSL_TRUSTSTORE_LOCATION", "/mnt/certs/truststore", null),
            new EnvVar("SENTRY_ENABLED", "true", null),
            new EnvVar("SENTRY_ENVIRONMENT", "prod", null),
            valueFromSecret("SENTRY_DSN", "sentry-secret", "gms_dsn"),
            valueFromSecret(
                "DATAHUB_TOKEN_SERVICE_SIGNING_KEY",
                "datahub-auth-secrets",
                "token_service_signing_key"),
            valueFromSecret(
                "DATAHUB_TOKEN_SERVICE_SALT", "datahub-auth-secrets", "token_service_salt"),
            valueFromSecret(
                "DATAHUB_SYSTEM_CLIENT_SECRET", "datahub-auth-secrets", "system_client_secret"),
            valueFromSecret(
                "SECRET_SERVICE_ENCRYPTION_KEY", "datahub-encryption-secrets", "encryption_key"),
            valueFromSecret(SSL_PASSWORD_ENV, "ssl-config", "keystore_password"),
            valueFromSecret(
                "SPRING_KAFKA_PROPERTIES_SSL_KEY_PASSWORD", "ssl-config", "keystore_password"),
            valueFromSecret(
                "SPRING_KAFKA_PROPERTIES_SSL_TRUSTSTORE_PASSWORD",
                "ssl-config",
                "truststore_password"),
            b.withName("POD_NAME")
                .withNewValueFrom()
                .withNewFieldRef()
                .withFieldPath("metadata.name")
                .endFieldRef()
                .endValueFrom()
                .build());
    return new DeploymentBuilder()
        .withNewMetadata()
        .withName(GMS_DEPLOYMENT)
        .withNamespace(NAMESPACE)
        .endMetadata()
        .withNewSpec()
        .withReplicas(1)
        .withNewTemplate()
        .withNewSpec()
        .addNewContainer()
        .withName("datahub-gms")
        .withEnv(envs)
        .endContainer()
        .endSpec()
        .endTemplate()
        .endSpec()
        .build();
  }

  /** Same as captureOperator but for the realistic deployment shape. */
  private static UnaryOperator<Deployment> captureOperatorForRealistic(
      Deployment original, Map<String, String> updates) {
    AtomicReference<UnaryOperator<Deployment>> captured = new AtomicReference<>();
    KubernetesClient client = mock(KubernetesClient.class);
    AppsAPIGroupDSL apps = mock(AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    RollableScalableResource<Deployment> resource = mock(RollableScalableResource.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.withName(GMS_DEPLOYMENT)).thenReturn(resource);
    when(resource.get()).thenReturn(original);
    when(resource.edit(any(UnaryOperator.class)))
        .thenAnswer(
            inv -> {
              captured.set(inv.getArgument(0));
              return original;
            });
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    accessor.setDeploymentEnv(GMS_DEPLOYMENT, NAMESPACE, updates);
    UnaryOperator<Deployment> op = captured.get();
    assertNotNull(op);
    return op;
  }

  /**
   * Run the same JSON Patch diff against the realistic GMS env shape. With ~30 env vars and
   * HashMap-driven reordering, fabric8 may emit very different patch ops than the small case —
   * including spurious "replace" operations that drop fields.
   */
  @Test
  public void jsonPatchDiffWithRealisticGmsEnv() {
    Deployment original = buildRealisticGmsDeployment();
    Map<String, String> scaleDownUpdates =
        Map.of(
            "PRE_PROCESS_HOOKS_UI_ENABLED", "false",
            "MCE_CONSUMER_ENABLED", "false",
            "MAE_CONSUMER_ENABLED", "false",
            "PE_CONSUMER_ENABLED", "false",
            "MCP_CONSUMER_ENABLED", "false");
    UnaryOperator<Deployment> op = captureOperatorForRealistic(original, scaleDownUpdates);

    KubernetesSerialization serialization = new KubernetesSerialization();
    Deployment cloned = serialization.clone(original);
    Deployment modified = op.apply(cloned);

    String diff = PatchUtils.jsonDiff(original, modified, false, serialization);

    System.out.println(
        "[hypothesis-check] Realistic GMS JSON Patch diff (length " + diff.length() + "):");
    System.out.println(diff);

    // Sanity-check that valueFrom env vars survive in the modified object itself.
    for (String secretEnv :
        List.of(
            SSL_PASSWORD_ENV,
            "SPRING_KAFKA_PROPERTIES_SSL_KEY_PASSWORD",
            "SPRING_KAFKA_PROPERTIES_SSL_TRUSTSTORE_PASSWORD",
            "DATAHUB_TOKEN_SERVICE_SIGNING_KEY",
            "DATAHUB_TOKEN_SERVICE_SALT",
            "DATAHUB_SYSTEM_CLIENT_SECRET",
            "SECRET_SERVICE_ENCRYPTION_KEY",
            "SENTRY_DSN")) {
      EnvVar e = findEnv(modified, secretEnv);
      assertNotNull(e.getValueFrom(), "valueFrom dropped on " + secretEnv + " by operator");
      assertNull(e.getValue(), "literal value populated on " + secretEnv + " by operator");
    }

    // Decisive assertion: fabric8's JSON Patch diff is index-based. When HashMap reorders the
    // env list, it emits "replace name + remove valueFrom + add value" patches against array
    // positions whose contents changed. The K8s API server applies that faithfully, destroying
    // the secret binding at every reshuffled position.
    assertFalse(
        diff.contains("\"remove\",\"path\":\"/spec/template/spec/containers/0/env/")
            && diff.contains("/valueFrom\""),
        "JSON Patch contains a 'remove /env/N/valueFrom' op — this destroys a secret/configmap"
            + " reference at that array position. Diff: "
            + diff);
  }

  /**
   * STAGE 3 (decisive): fabric8 computes a JSON Patch (RFC 6902) between the original (server) and
   * the modified resource using PatchUtils.jsonDiff, then submits that patch. If this diff contains
   * any operation against the unrelated SSL EnvVar — replace, remove, or add for its valueFrom or
   * value — the API server will mutate it. That is the failure mode we expect on AWS deployments
   * where SPRING_KAFKA_PROPERTIES_SSL_KEYSTORE_PASSWORD is sourced from a Secret.
   */
  @Test
  public void jsonPatchDiffDoesNotTouchUnrelatedValueFromEnv() {
    Deployment original = buildGmsDeployment();
    UnaryOperator<Deployment> op = captureOperator(original);

    KubernetesSerialization serialization = new KubernetesSerialization();
    Deployment cloned = serialization.clone(original);
    Deployment modified = op.apply(cloned);

    String diff = PatchUtils.jsonDiff(original, modified, true, serialization);

    System.out.println("[hypothesis-check] JSON Patch diff for setDeploymentEnv:");
    System.out.println(diff);

    assertTrue(
        diff.contains(UI_HOOKS_ENV) || diff.contains("\"false\""),
        "diff should describe the change to the UI hooks env var; got: " + diff);

    boolean diffMentionsSslEnv = diff.contains(SSL_PASSWORD_ENV);
    boolean diffMentionsValueFrom = diff.toLowerCase().contains("valuefrom");
    boolean diffMentionsSecretKeyRef = diff.toLowerCase().contains("secretkeyref");

    assertFalse(
        diffMentionsSslEnv,
        "JSON Patch references "
            + SSL_PASSWORD_ENV
            + " — the secret-backed env will be mutated by the API server. Diff: "
            + diff);
    assertFalse(
        diffMentionsValueFrom,
        "JSON Patch operates on a valueFrom field — secret references will be lost. Diff: " + diff);
    assertFalse(
        diffMentionsSecretKeyRef,
        "JSON Patch operates on a secretKeyRef — secret references will be lost. Diff: " + diff);
  }
}

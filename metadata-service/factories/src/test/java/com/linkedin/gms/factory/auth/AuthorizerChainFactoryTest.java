package com.linkedin.gms.factory.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.datahub.authentication.group.GroupService;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.datahub.authorization.DataHubAuthorizer;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.ProductionConfigContextRunner;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Loads {@link AuthorizerChainFactory} (and the {@link DataHubAuthorizerFactory} it imports) with
 * the production {@code application.yaml} and checks how {@code authorization.defaultAuthorizer.*}
 * and {@code datahub.plugin.auth.path} shape the chain every request is authorized against.
 */
public class AuthorizerChainFactoryTest {

  private static final List<Path> TEMP_DIRS = new CopyOnWriteArrayList<>();

  private static final String PLUGIN_JAR = "authorizer.jar";

  private static final OperationContext SYSTEM_OP_CONTEXT =
      TestOperationContexts.systemContextNoValidate();

  private final ApplicationContextRunner contextRunner =
      ProductionConfigContextRunner.create()
          .withUserConfiguration(AuthorizerChainFactory.class)
          .withBean("systemOperationContext", OperationContext.class, () -> SYSTEM_OP_CONTEXT)
          .withBean(
              "systemEntityClient", SystemEntityClient.class, () -> mock(SystemEntityClient.class))
          .withBean("groupService", GroupService.class, () -> mock(GroupService.class))
          // DataHubAuthorizer has no shutdown hook; 0 keeps it from starting a policy refresh
          // thread per context that outlives the test.
          .withPropertyValues("authorization.defaultAuthorizer.cacheRefreshIntervalSecs=0")
          // Production points at /etc/datahub/plugins/auth; keep tests off the host filesystem.
          .withPropertyValues("datahub.plugin.auth.path=" + emptyDir("no-auth-plugins"));

  @Test
  public void testDefaultAuthorizerIsInTheChainByDefault() {
    contextRunner.run(
        context -> {
          assertThat(context).hasNotFailed();
          AuthorizerChain chain = context.getBean("authorizerChain", AuthorizerChain.class);
          DataHubAuthorizer dataHubAuthorizer =
              context.getBean("dataHubAuthorizer", DataHubAuthorizer.class);

          assertThat(chain.getDefaultAuthorizer()).isSameAs(dataHubAuthorizer);
          assertThat(dataHubAuthorizer.mode())
              .isEqualTo(DataHubAuthorizer.AuthorizationMode.DEFAULT);
          // The system actor is allowed by DataHubAuthorizer, so ALLOW proves the chain consults
          // it.
          assertThat(authorizeSystemActor(context)).isEqualTo(AuthorizationResult.Type.ALLOW);
        });
  }

  /**
   * {@code AUTH_POLICIES_ENABLED=false} turns DataHubAuthorizer into ALLOW_ALL mode but also leaves
   * it out of the chain, so with no plugin authorizers the chain has nothing to ask and denies.
   *
   * <p>KNOWN BUG, not intended behaviour: {@code docs/authorization/policies.md} says this flag
   * allows all actions. This test records today's wiring so the fix is a deliberate change; it is
   * tracked as an open bug and that fix must update this assertion.
   */
  @Test
  public void testDisabledDefaultAuthorizerIsLeftOutOfTheChain() {
    contextRunner
        .withPropertyValues("authorization.defaultAuthorizer.enabled=false")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              AuthorizerChain chain = context.getBean("authorizerChain", AuthorizerChain.class);
              DataHubAuthorizer dataHubAuthorizer =
                  context.getBean("dataHubAuthorizer", DataHubAuthorizer.class);

              assertThat(chain.getDefaultAuthorizer()).isSameAs(dataHubAuthorizer);
              assertThat(dataHubAuthorizer.mode())
                  .isEqualTo(DataHubAuthorizer.AuthorizationMode.ALLOW_ALL);
              assertThat(authorizeSystemActor(context)).isEqualTo(AuthorizationResult.Type.DENY);
            });
  }

  @Test
  public void testDisabledPluginAuthorizerIsSkipped() throws IOException {
    Path pluginDir = pluginDirWith("disabled-authorizer", false);
    contextRunner
        .withPropertyValues("datahub.plugin.auth.path=" + pluginDir)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(authorizeSystemActor(context)).isEqualTo(AuthorizationResult.Type.ALLOW);
            });
  }

  @Test
  public void testEnabledPluginAuthorizerIsLoadedFromTheConfiguredPath() throws IOException {
    // The plugin's jar is empty, so instantiating its class fails startup. That proves the factory
    // read config.yml from datahub.plugin.auth.path and tried to load the enabled authorizer.
    Path pluginDir = pluginDirWith("enabled-authorizer", true);
    contextRunner
        .withPropertyValues("datahub.plugin.auth.path=" + pluginDir)
        .run(
            context ->
                assertThat(context)
                    .hasFailed()
                    .getFailure()
                    .rootCause()
                    .isInstanceOf(ClassNotFoundException.class));
  }

  private static AuthorizationResult.Type authorizeSystemActor(ApplicationContext context) {
    AuthorizerChain chain = context.getBean("authorizerChain", AuthorizerChain.class);
    AuthorizationRequest request =
        new AuthorizationRequest(
            SYSTEM_OP_CONTEXT.getAuthentication().getActor().toUrnStr(),
            "MANAGE_POLICIES",
            Optional.empty(),
            List.of());
    return chain.authorize(request).getType();
  }

  private static Path pluginDirWith(String pluginName, boolean enabled) throws IOException {
    Path dir = emptyDir("auth-plugins");
    Path pluginHome = Files.createDirectories(dir.resolve(pluginName));
    // Plugin config loading requires the jar to exist; an empty one holds no authorizer class.
    try (JarOutputStream jar =
        new JarOutputStream(Files.newOutputStream(pluginHome.resolve(PLUGIN_JAR)))) {
      jar.flush();
    }
    Files.writeString(
        dir.resolve("config.yml"),
        String.join(
            "\n",
            "plugins:",
            "  - name: \"" + pluginName + "\"",
            "    type: \"authorizer\"",
            "    enabled: \"" + enabled + "\"",
            "    params:",
            "      className: \"com.example.MissingAuthorizer\"",
            "      jarFileName: \"" + PLUGIN_JAR + "\"",
            ""));
    return dir;
  }

  private static Path emptyDir(String prefix) {
    try {
      Path dir = Files.createTempDirectory(prefix);
      TEMP_DIRS.add(dir);
      return dir;
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @AfterClass(alwaysRun = true)
  public void deleteTempDirs() throws IOException {
    for (Path dir : TEMP_DIRS) {
      try (Stream<Path> paths = Files.walk(dir)) {
        for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
          Files.deleteIfExists(path);
        }
      }
    }
    TEMP_DIRS.clear();
  }
}

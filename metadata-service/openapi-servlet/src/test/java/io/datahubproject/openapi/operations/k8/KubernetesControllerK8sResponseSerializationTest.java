package io.datahubproject.openapi.operations.k8;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests the KubernetesController k8sResponse serialization failure path (500 and "Serialization
 * failed" when ObjectMapper.writeValueAsString throws JsonProcessingException).
 */
@SpringBootTest(
    classes = KubernetesControllerK8sResponseSerializationTest.SerializationTestConfig.class)
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class KubernetesControllerK8sResponseSerializationTest
    extends AbstractTestNGSpringContextTests {

  private static final String NAMESPACE = "datahub";
  private static final String BASE_PATH = "/openapi/operations/k8";

  @Autowired private MockMvc mockMvc;
  @Autowired private KubernetesClient kubernetesClient;

  @BeforeMethod
  public void setupMocks() {
    Authentication authentication = mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));
    AuthenticationContext.setAuthentication(authentication);

    Config config = mock(Config.class);
    when(config.getNamespace()).thenReturn(NAMESPACE);
    when(kubernetesClient.getConfiguration()).thenReturn(config);
  }

  @Test
  public void testK8sResponseReturns500WhenSerializationFails() throws Exception {
    Deployment deployment =
        new DeploymentBuilder()
            .withNewMetadata()
            .withName("test-deployment")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();
    DeploymentList deploymentList = new DeploymentList();
    deploymentList.setItems(List.of(deployment));

    MixedOperation deploymentsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    when(kubernetesClient.apps()).thenReturn(mock(AppsAPIGroupDSL.class));
    when(kubernetesClient.apps().deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.list()).thenReturn(deploymentList);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/deployments")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isInternalServerError())
        .andExpect(content().string(org.hamcrest.Matchers.containsString("Serialization failed")));
  }

  @SpringBootConfiguration
  @Import({SerializationTestK8sConfig.class, TracingInterceptor.class, KubernetesController.class})
  static class SerializationTestConfig {}

  @TestConfiguration
  public static class SerializationTestK8sConfig {

    @Bean
    @Primary
    public KubernetesClient kubernetesClient() {
      return mock(KubernetesClient.class);
    }

    /** ObjectMapper that throws on writeValueAsString to exercise k8sResponse error path. */
    @Bean(name = "kubernetesObjectMapper")
    @Primary
    public ObjectMapper kubernetesObjectMapper() {
      return new ObjectMapper() {
        @Override
        public String writeValueAsString(Object value) throws JsonProcessingException {
          throw new JsonProcessingException("test serialization failure") {};
        }
      };
    }

    @Bean
    public ObjectMapper objectMapper() {
      return new ObjectMapper();
    }

    @Bean
    @Primary
    public SystemTelemetryContext systemTelemetryContext() {
      return mock(SystemTelemetryContext.class);
    }

    @Bean(name = "systemOperationContext")
    public OperationContext systemOperationContext(
        @Qualifier("objectMapper") ObjectMapper objectMapper,
        SystemTelemetryContext systemTelemetryContext) {
      return TestOperationContexts.systemContextTraceNoSearchAuthorization(
          () -> ObjectMapperContext.builder().objectMapper(objectMapper).build(),
          () -> systemTelemetryContext);
    }

    @Bean
    @Primary
    public AuthorizerChain authorizerChain() {
      AuthorizerChain authorizerChain = mock(AuthorizerChain.class);
      Authentication authentication = mock(Authentication.class);
      when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));
      when(authorizerChain.authorize(any()))
          .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
      AuthenticationContext.setAuthentication(authentication);
      return authorizerChain;
    }
  }
}

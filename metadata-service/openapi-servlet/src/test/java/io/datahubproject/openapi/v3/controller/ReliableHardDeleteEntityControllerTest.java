package io.datahubproject.openapi.v3.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entity.versioning.EntityVersioningServiceFactory;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.service.async.delete.ReliableHardDelete;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.openapi.config.GlobalControllerExceptionHandler;
import io.datahubproject.openapi.config.SpringWebConfig;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.openapi.test.AuthorizerChainTestSupport;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest(classes = {SpringWebConfig.class})
@ComponentScan(basePackages = {"io.datahubproject.openapi.v3.controller.EntityController"})
@Import({
  SpringWebConfig.class,
  TracingInterceptor.class,
  EntityController.class,
  EntityControllerTest.EntityControllerTestConfig.class,
  EntityVersioningServiceFactory.class,
  GlobalControllerExceptionHandler.class,
})
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class ReliableHardDeleteEntityControllerTest extends AbstractTestNGSpringContextTests {
  private static final Urn URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:testPlatform,my_table,PROD)");
  private static final String PATH = "/openapi/v3/entity/dataset/" + URN;

  @Autowired private MockMvc mockMvc;
  @Autowired private AuthorizerChain authorizerChain;

  @MockitoBean private ConfigurationProvider configurationProvider;
  @MockitoBean private EntityServiceImpl mockEntityService;
  @MockitoBean private SearchService mockSearchService;
  @MockitoBean private TimeseriesAspectService mockTimeseriesAspectService;
  @MockitoBean private SystemTelemetryContext systemTelemetryContext;
  @MockitoBean private ReliableHardDelete reliableHardDelete;

  @BeforeMethod
  public void setup() {
    reset(mockEntityService, reliableHardDelete, authorizerChain);
    AuthorizerChainTestSupport.stubAllowViaOperationContextAuthorizer(authorizerChain);
  }

  @Test
  public void enabledEntityDeleteUsesTheReliableDelete() throws Exception {
    when(reliableHardDelete.isEnabled()).thenReturn(true);

    mockMvc
        .perform(MockMvcRequestBuilders.delete(PATH).accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    verify(reliableHardDelete).delete(any(), eq(URN));
    verify(mockEntityService, never()).deleteUrn(any(), any(Urn.class));
  }

  @Test
  public void enabledKeyAspectDeleteUsesTheReliableDelete() throws Exception {
    when(reliableHardDelete.isEnabled()).thenReturn(true);

    mockMvc
        .perform(
            MockMvcRequestBuilders.delete(PATH)
                .param("aspects", "datasetKey")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    verify(reliableHardDelete).delete(any(), eq(URN));
  }

  @Test
  public void enabledAspectDeleteKeepsTodaysPath() throws Exception {
    when(reliableHardDelete.isEnabled()).thenReturn(true);

    mockMvc
        .perform(
            MockMvcRequestBuilders.delete(PATH)
                .param("aspects", "status")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    verify(mockEntityService)
        .deleteAspect(any(), eq(URN.toString()), eq("status"), anyMap(), eq(true));
    verify(reliableHardDelete, never()).delete(any(), any());
  }

  @Test
  public void enabledClearKeepsTodaysPath() throws Exception {
    when(reliableHardDelete.isEnabled()).thenReturn(true);

    mockMvc
        .perform(
            MockMvcRequestBuilders.delete(PATH)
                .param("clear", "true")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    verify(reliableHardDelete, never()).delete(any(), any());
  }

  @Test
  public void disabledEntityDeleteIsTodays() throws Exception {
    when(reliableHardDelete.isEnabled()).thenReturn(false);

    mockMvc
        .perform(MockMvcRequestBuilders.delete(PATH).accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    verify(mockEntityService).deleteUrn(any(), eq(URN));
    verify(reliableHardDelete, never()).delete(any(), any());
  }

  /** The flag never moves the delete ahead of the DELETE privilege check. */
  @Test
  public void unauthorizedEntityDeleteStartsNothing() throws Exception {
    when(reliableHardDelete.isEnabled()).thenReturn(true);
    reset(authorizerChain);
    final AuthorizationResult denied =
        new AuthorizationResult(null, AuthorizationResult.Type.DENY, "");
    when(authorizerChain.authorize(any(AuthorizationRequest.class))).thenReturn(denied);
    when(authorizerChain.authorize(
            any(AuthorizationRequest.class), any(Map.class), any(OperationContext.class)))
        .thenReturn(denied);

    mockMvc
        .perform(MockMvcRequestBuilders.delete(PATH).accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isForbidden());

    verify(reliableHardDelete, never()).delete(any(), any());
    verify(mockEntityService, never()).deleteUrn(any(), any(Urn.class));
  }

  @Test
  public void failedReliableDeleteAnswersAServerError() throws Exception {
    when(reliableHardDelete.isEnabled()).thenReturn(true);
    when(reliableHardDelete.delete(any(), eq(URN)))
        .thenThrow(new IllegalStateException("did not finish within 55 seconds"));

    mockMvc
        .perform(MockMvcRequestBuilders.delete(PATH).accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is5xxServerError());
  }
}

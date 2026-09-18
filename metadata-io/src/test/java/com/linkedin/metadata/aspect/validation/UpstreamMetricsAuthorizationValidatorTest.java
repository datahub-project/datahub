package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.UPSTREAM_METRICS_ASPECT_NAME;
import static com.linkedin.metadata.authorization.ApiGroup.LINEAGE;
import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Edge;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.UpstreamMetrics;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpstreamMetricsAuthorizationValidatorTest {

  private static final Urn CHART_URN = UrnUtils.getUrn("urn:li:chart:(looker,revenue)");
  private static final Urn METRIC_URN =
      UrnUtils.getUrn("urn:li:metric:(urn:li:dataPlatform:looker,kpi.revenue,PROD)");

  private final EntityRegistry registry =
      TestOperationContexts.systemContextNoSearchAuthorization().getEntityRegistry();

  private UpstreamMetricsAuthorizationValidator validator;
  private AuthorizationSession mockAuthSession;
  private MockedStatic<AuthUtil> authUtilMockedStatic;
  private io.datahubproject.metadata.context.RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    authUtilMockedStatic = Mockito.mockStatic(AuthUtil.class);
    validator = new UpstreamMetricsAuthorizationValidator();
    validator.setConfig(
        AspectPluginConfig.builder()
            .className(UpstreamMetricsAuthorizationValidator.class.getName())
            .enabled(true)
            .supportedOperations(List.of("UPSERT"))
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName("*")
                        .aspectName(UPSTREAM_METRICS_ASPECT_NAME)
                        .build()))
            .build());
    mockAuthSession = Mockito.mock(AuthorizationSession.class);
    CachingAspectRetriever mockAspectRetriever = Mockito.mock(CachingAspectRetriever.class);
    Mockito.when(mockAspectRetriever.getEntityRegistry()).thenReturn(registry);
    Mockito.when(mockAspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenReturn(Map.of());
    retrieverContext =
        io.datahubproject.metadata.context.RetrieverContext.builder()
            .searchRetriever(Mockito.mock(SearchRetriever.class))
            .graphRetriever(Mockito.mock(GraphRetriever.class))
            .cachingAspectRetriever(mockAspectRetriever)
            .build();
  }

  @AfterMethod
  public void tearDown() {
    authUtilMockedStatic.close();
  }

  @Test
  public void testDenyWithoutLineagePrivilegeOnConsumer() {
    stubLineageAuth(false);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            OperationFingerprint.EMPTY,
            Collections.singletonList(buildItem()),
            retrieverContext,
            mockAuthSession);

    Assert.assertTrue(result.findAny().isPresent());
  }

  @Test
  public void testAllowWithLineagePrivilegeOnBothEnds() {
    stubLineageAuth(true);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            OperationFingerprint.EMPTY,
            Collections.singletonList(buildItem()),
            retrieverContext,
            mockAuthSession);

    Assert.assertTrue(result.findAny().isEmpty());
  }

  private void stubLineageAuth(boolean allowed) {
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorizedUrns(
                    eq(mockAuthSession), eq(LINEAGE), eq(UPDATE), any(Collection.class)))
        .thenReturn(allowed);
  }

  private TestMCP buildItem() {
    UpstreamMetrics aspect =
        new UpstreamMetrics().setMetrics(new EdgeArray(new Edge().setDestinationUrn(METRIC_URN)));
    return TestMCP.ofOneUpsertItem(CHART_URN, aspect, registry).stream()
        .map(i -> (TestMCP) i)
        .findFirst()
        .get();
  }
}

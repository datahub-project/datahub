package io.datahubproject.openapi.v2.delegates;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.usage.instrumentation.UsageMetricsSessionEnricher;
import com.linkedin.metadata.usage.store.UsageAggregationStore;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.openapi.generated.DatasetEntityRequestV2;
import io.datahubproject.openapi.generated.DatasetEntityResponseV2;
import io.datahubproject.openapi.generated.ScrollDatasetEntityResponseV2;
import io.datahubproject.openapi.v1.entities.EntitiesApiService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import jakarta.servlet.http.HttpServletRequest;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class EntityApiDelegateImplDeleteAspectUsageTest {

  private static final String TEST_URN =
      "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)";

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Test
  public void deleteAspectRecordsSingleApiCall() throws Exception {
    AtomicInteger recordRequestCount = new AtomicInteger();
    UsageAggregationStore usageStore =
        new UsageAggregationStore() {
          @Override
          public boolean recordRequest(@Nonnull OperationContext opContext) {
            recordRequestCount.incrementAndGet();
            return true;
          }

          @Override
          public void recordResponse(
              @Nonnull OperationContext opContext, @Nullable Long outputBytes) {}

          @Override
          public void flush(@Nonnull com.linkedin.metadata.usage.flush.FlushTrigger trigger) {}
        };
    UsageMetricsSessionEnricher sessionEnricher = new UsageMetricsSessionEnricher(usageStore, true);

    OperationContext systemOperationContext =
        TestOperationContexts.Builder.builder()
            .configSupplier(
                () ->
                    OperationContextConfig.builder()
                        .sessionContextEnricher(sessionEnricher)
                        .build())
            .systemTelemetryContextSupplier(() -> SystemTelemetryContext.TEST.toBuilder().build())
            .buildSystemContext();

    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getHeader(any())).thenReturn("");

    EntityService<ChangeItemImpl> entityService = mock(EntityService.class);
    when(entityService.deleteUrn(any(), any())).thenReturn(mock(RollbackRunResult.class));

    AuthorizerChain authorizerChain = mock(AuthorizerChain.class);
    EntitiesApiService entitiesApiService =
        new EntitiesApiService(
            systemOperationContext, entityService, new ObjectMapper(), authorizerChain);

    EntityApiDelegateImpl<
            DatasetEntityRequestV2, DatasetEntityResponseV2, ScrollDatasetEntityResponseV2>
        delegate =
            new EntityApiDelegateImpl<>(
                systemOperationContext,
                request,
                entityService,
                mock(SearchService.class),
                entitiesApiService,
                authorizerChain,
                DatasetEntityRequestV2.class,
                DatasetEntityResponseV2.class,
                ScrollDatasetEntityResponseV2.class);

    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, "datahub"), "Basic test");

    try (MockedStatic<AuthenticationContext> authContext =
            Mockito.mockStatic(AuthenticationContext.class);
        MockedStatic<AuthUtil> authUtil = Mockito.mockStatic(AuthUtil.class)) {
      authContext.when(AuthenticationContext::getAuthentication).thenReturn(authentication);
      authUtil
          .when(() -> AuthUtil.isAPIAuthorizedEntityUrns(any(), any(), anySet()))
          .thenReturn(true);

      delegate.deleteAspect(TEST_URN, "status");
    }

    assertEquals(recordRequestCount.get(), 1);
    verify(entityService).deleteAspect(any(), eq(TEST_URN), eq("status"), any(), eq(false));
    verify(entityService).deleteUrn(any(), eq(UrnUtils.getUrn(TEST_URN)));
  }
}

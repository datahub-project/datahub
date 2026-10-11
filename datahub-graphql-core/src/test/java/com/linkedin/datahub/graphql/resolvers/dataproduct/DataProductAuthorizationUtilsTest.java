package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.service.DataProductService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataProductAuthorizationUtilsTest {

  private static final Urn PRODUCT_URN = UrnUtils.getUrn("urn:li:dataProduct:campaign");
  private static final Urn DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:marketing");

  private QueryContext context;
  private AspectRetriever aspectRetriever;

  @BeforeMethod
  public void setUp() {
    context = mock(QueryContext.class);
    OperationContext operationContext = mock(OperationContext.class);
    aspectRetriever = mock(AspectRetriever.class);
    when(context.getOperationContext()).thenReturn(operationContext);
    when(operationContext.getAspectRetriever()).thenReturn(aspectRetriever);
  }

  private static Domains domainsWith(Urn domainUrn) {
    return new Domains().setDomains(new UrnArray(List.of(domainUrn)));
  }

  @Test
  public void testLiveDomainProbeFailureDegradesToOrphanFallback() {
    when(aspectRetriever.entityExists(any(), any())).thenThrow(new RuntimeException("gms down"));

    try (MockedStatic<AuthorizationUtils> auth = Mockito.mockStatic(AuthorizationUtils.class)) {
      auth.when(() -> AuthorizationUtils.isAuthorized(any(), any(), any(), any())).thenReturn(true);
      auth.when(() -> AuthorizationUtils.canManageDomains(any())).thenReturn(false);

      assertTrue(
          DataProductAuthorizationUtils.isAuthorizedToManageDataProduct(
              context, PRODUCT_URN, domainsWith(DOMAIN_URN)));

      auth.verify(
          () ->
              AuthorizationUtils.isAuthorized(
                  eq(context), eq(DOMAIN_URN.getEntityType()), eq(DOMAIN_URN.toString()), any()),
          never());
    }
  }

  @Test
  public void testLiveDomainProbeFailureWithoutOrphanPrivilegeIsDenied() {
    when(aspectRetriever.entityExists(any(), any())).thenThrow(new RuntimeException("gms down"));

    try (MockedStatic<AuthorizationUtils> auth = Mockito.mockStatic(AuthorizationUtils.class)) {
      auth.when(() -> AuthorizationUtils.isAuthorized(any(), any(), any(), any()))
          .thenReturn(false);
      auth.when(() -> AuthorizationUtils.canManageDomains(any())).thenReturn(false);

      assertFalse(
          DataProductAuthorizationUtils.isAuthorizedToManageDataProduct(
              context, PRODUCT_URN, domainsWith(DOMAIN_URN)));
    }
  }

  @Test
  public void testMembershipUsesOrphanFallbackWhenDomainMissing() {
    DataProductService service = mock(DataProductService.class);
    when(service.getDataProductDomains(any(), eq(PRODUCT_URN))).thenReturn(domainsWith(DOMAIN_URN));
    when(aspectRetriever.entityExists(any(), any())).thenReturn(Map.of(DOMAIN_URN, false));

    try (MockedStatic<AuthorizationUtils> auth = Mockito.mockStatic(AuthorizationUtils.class)) {
      auth.when(() -> AuthorizationUtils.isAuthorized(any(), any(), any(), any())).thenReturn(true);
      auth.when(() -> AuthorizationUtils.canManageDomains(any())).thenReturn(false);

      assertTrue(
          DataProductAuthorizationUtils.isAuthorizedToChangeMembershipFromProductSide(
              context, service, PRODUCT_URN));

      auth.verify(
          () ->
              AuthorizationUtils.isAuthorized(
                  eq(context), eq(DOMAIN_URN.getEntityType()), eq(DOMAIN_URN.toString()), any()),
          never());
    }
  }
}

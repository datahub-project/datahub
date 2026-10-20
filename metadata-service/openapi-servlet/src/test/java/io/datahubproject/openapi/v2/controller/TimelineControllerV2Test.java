package io.datahubproject.openapi.v2.controller;

import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.authorization.EntityAuthorizationUtils;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.v1.timeline.TimelineControllerV1;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Collection;
import java.util.Set;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TimelineControllerV2Test {

  private static final Urn HIDDEN_URN = UrnUtils.getUrn("urn:li:chart:(looker,hidden_chart)");

  private TimelineService timelineService;
  private MockedStatic<AuthenticationContext> authenticationContextMock;
  private MockedStatic<OperationContext> operationContextMock;
  private MockedStatic<EntityAuthorizationUtils> entityAuthMock;

  @BeforeMethod
  public void setUp() {
    timelineService = mock(TimelineService.class);
    Authentication authentication = mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "testuser"));
    authenticationContextMock = mockStatic(AuthenticationContext.class);
    authenticationContextMock
        .when(AuthenticationContext::getAuthentication)
        .thenReturn(authentication);
    operationContextMock = mockStatic(OperationContext.class);
    operationContextMock
        .when(() -> OperationContext.asSession(any(), any(), any(), any(), anyBoolean()))
        .thenReturn(mock(OperationContext.class));
    entityAuthMock = mockStatic(EntityAuthorizationUtils.class);
    entityAuthMock
        .when(
            () ->
                EntityAuthorizationUtils.isAPIAuthorizedEntityUrns(
                    any(), eq(READ), argThat((Collection<Urn> urns) -> urns.contains(HIDDEN_URN))))
        .thenReturn(false);
  }

  @AfterMethod
  public void tearDown() {
    authenticationContextMock.close();
    operationContextMock.close();
    entityAuthMock.close();
  }

  @Test
  public void testV2ReadsReadDeniedUrnAsMissingOne() throws Exception {
    TimelineControllerV2 controller =
        new TimelineControllerV2(
            mock(OperationContext.class),
            timelineService,
            mock(AuthorizerChain.class),
            mock(EntityClient.class));
    ReflectionTestUtils.setField(controller, "restApiAuthorizationEnabled", false);

    controller.getTimeline(
        mock(HttpServletRequest.class),
        HIDDEN_URN.toString(),
        -1,
        0,
        false,
        Set.of(ChangeCategory.TAG),
        false);

    verifyReadAsMissing();
  }

  @Test
  public void testV1ReadsReadDeniedUrnAsMissingOne() throws Exception {
    TimelineControllerV1 controller =
        new TimelineControllerV1(
            mock(OperationContext.class), timelineService, mock(AuthorizerChain.class), false);

    controller.getTimeline(
        mock(HttpServletRequest.class),
        HIDDEN_URN.toString(),
        -1,
        0,
        false,
        Set.of(ChangeCategory.TAG));

    verifyReadAsMissing();
  }

  private void verifyReadAsMissing() throws Exception {
    verify(timelineService)
        .getTimeline(
            any(),
            eq(EntityUtils.toMissingUrn(HIDDEN_URN)),
            eq(Set.of(ChangeCategory.TAG)),
            anyLong(),
            anyLong(),
            any(),
            any(),
            anyBoolean());
    verifyNoMoreInteractions(timelineService);
  }
}

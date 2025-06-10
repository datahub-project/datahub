package com.linkedin.datahub.graphql.resolvers.notification;

import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.FormNotificationRecipientInput;
import com.linkedin.datahub.graphql.generated.FormNotificationType;
import com.linkedin.datahub.graphql.generated.NotificationRecipientType;
import com.linkedin.datahub.graphql.generated.SendFormNotificationRequestInput;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.PlatformEvent;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SendFormNotificationRequestResolverTest {

  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";
  private static final String TEST_RECIPIENT_ID = "test-recipient";

  private static final SendFormNotificationRequestInput TEST_INPUT = createTestInput();

  private static SendFormNotificationRequestInput createTestInput() {
    SendFormNotificationRequestInput input = new SendFormNotificationRequestInput();

    input.setType(FormNotificationType.BROADCAST_COMPLIANCE_FORM_PUBLISH);
    List<StringMapEntryInput> parameters = new ArrayList<>();
    StringMapEntryInput param = new StringMapEntryInput();
    param.setKey("key");
    param.setValue("value");
    parameters.add(param);
    input.setParameters(parameters);

    // Create recipient input
    List<FormNotificationRecipientInput> recipients = new ArrayList<>();
    FormNotificationRecipientInput recipient = new FormNotificationRecipientInput();
    recipient.setType(NotificationRecipientType.EMAIL);
    recipient.setId(TEST_RECIPIENT_ID);
    recipient.setActor(TEST_ACTOR_URN);
    recipient.setDisplayName("Test User");
    recipients.add(recipient);
    input.setRecipients(recipients);

    return input;
  }

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = initMockEntityClient(true);
    SendFormNotificationRequestResolver resolver =
        new SendFormNotificationRequestResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockSystemContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Boolean success = resolver.get(mockEnv).get();

    assertTrue(success);

    // Validate that we called producePlatformEvent
    Mockito.verify(mockClient, Mockito.times(1))
        .producePlatformEvent(
            any(OperationContext.class),
            Mockito.eq(Constants.NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            any(PlatformEvent.class));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityClient mockClient = initMockEntityClient(true);
    SendFormNotificationRequestResolver resolver =
        new SendFormNotificationRequestResolver(mockClient);

    // Execute resolver with non-system auth context
    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate that we did NOT call producePlatformEvent
    Mockito.verify(mockClient, Mockito.times(0))
        .producePlatformEvent(
            any(OperationContext.class),
            Mockito.eq(Constants.NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            any(PlatformEvent.class));
  }

  @Test
  public void testGetFailure() throws Exception {
    EntityClient mockClient = initMockEntityClient(false);
    SendFormNotificationRequestResolver resolver =
        new SendFormNotificationRequestResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockSystemContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private EntityClient initMockEntityClient(boolean shouldSucceed) throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);

    if (!shouldSucceed) {
      Mockito.doThrow(new RuntimeException())
          .when(client)
          .producePlatformEvent(
              any(OperationContext.class),
              Mockito.eq(Constants.NOTIFICATION_REQUEST_EVENT_NAME),
              Mockito.eq(null),
              any(PlatformEvent.class));
    }

    return client;
  }

  private QueryContext getMockSystemContext() {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    OperationContext mockOpContext = Mockito.mock(OperationContext.class);
    Mockito.when(mockOpContext.isSystemAuth()).thenReturn(true);
    Mockito.when(mockContext.getOperationContext()).thenReturn(mockOpContext);
    return mockContext;
  }
}

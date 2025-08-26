package com.datahub.notification.proxy;

import static org.mockito.Mockito.*;

import com.datahub.notification.NotificationContext;
import com.datahub.notification.NotificationSinkConfig;
import com.datahub.notification.NotificationTemplateType;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.metadata.integration.IntegrationsService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IntegrationsServiceProxySinkTest {

  @InjectMocks private EmailProxySink sink;

  @Mock private IntegrationsService integrationsService;

  @Mock private NotificationSinkConfig config;

  @Mock private OperationContext operationContext;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testType() {
    Assert.assertEquals(sink.type(), NotificationSinkType.EMAIL);
  }

  @Test
  public void testTemplates() {
    Collection<NotificationTemplateType> templates = sink.templates();
    Assert.assertNotNull(templates);
    Assert.assertTrue(templates.contains(NotificationTemplateType.CUSTOM));
  }

  @Test
  public void testRecipientTypes() {
    Collection<NotificationRecipientType> recipientTypes = sink.recipientTypes();
    Assert.assertNotNull(recipientTypes);
    Assert.assertTrue(recipientTypes.contains(NotificationRecipientType.EMAIL));
  }

  @Test
  public void testInit() {
    sink.init(operationContext, config);
    verify(config, times(1)).getIntegrationsService();
  }

  @Test
  public void testSend() throws Exception {
    NotificationRequest request = mock(NotificationRequest.class);
    NotificationContext context = mock(NotificationContext.class);
    doNothing().when(integrationsService).sendNotification(any(NotificationRequest.class));

    sink.send(mock(OperationContext.class), request, context);

    verify(integrationsService, times(1)).sendNotification(request);
  }
}

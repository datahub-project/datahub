package com.datahub.notification.recipient;

import com.linkedin.event.notification.NotificationSinkType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class NotificationRecipientBuildersTest {

  private NotificationRecipientBuilders recipientBuilders;
  private Map<NotificationSinkType, NotificationRecipientBuilder> buildersMap;

  @BeforeMethod
  public void setUp() {
    buildersMap = new HashMap<>();
    // Assuming NotificationRecipientBuilder is an interface, you might need to mock it.
    NotificationRecipientBuilder emailBuilder = Mockito.mock(NotificationRecipientBuilder.class);

    buildersMap.put(NotificationSinkType.EMAIL, emailBuilder);

    recipientBuilders = new NotificationRecipientBuilders(buildersMap);
  }

  @Test
  public void testGetBuilder() {
    NotificationRecipientBuilder builder = recipientBuilders.getBuilder(NotificationSinkType.EMAIL);
    Assert.assertNotNull(builder);
    Assert.assertEquals(builder, buildersMap.get(NotificationSinkType.EMAIL));

    NotificationRecipientBuilder missingBuilder =
        recipientBuilders.getBuilder(NotificationSinkType.SLACK);
    Assert.assertNull(missingBuilder);
  }

  @Test
  public void testListBuilders() {
    List<NotificationRecipientBuilder> builderList = recipientBuilders.listBuilders();
    Assert.assertNotNull(builderList);
    Assert.assertEquals(builderList.size(), buildersMap.values().size());
    // Ensure all builders in the map are in the returned list
    for (NotificationRecipientBuilder builder : builderList) {
      Assert.assertTrue(buildersMap.containsValue(builder));
    }
  }
}

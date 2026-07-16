package com.linkedin.metadata.datahubusage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.testng.annotations.Test;

public class DataHubUsageEventTypeTest {

  @Test
  public void testGetTypeMethod() {
    // Test that getType method works for various event types
    assertEquals(
        DataHubUsageEventType.getType("PageViewEvent"), DataHubUsageEventType.PAGE_VIEW_EVENT);
    assertEquals(DataHubUsageEventType.getType("LogInEvent"), DataHubUsageEventType.LOG_IN_EVENT);
    assertEquals(
        DataHubUsageEventType.getType("WelcomeToDataHubModalViewEvent"),
        DataHubUsageEventType.WELCOME_TO_DATAHUB_MODAL_VIEW_EVENT);

    // Test the new enum value
    assertEquals(
        DataHubUsageEventType.getType("ProductTourButtonClickEvent"),
        DataHubUsageEventType.PRODUCT_TOUR_BUTTON_CLICK_EVENT);

    // Test case insensitive matching
    assertEquals(
        DataHubUsageEventType.getType("producttourButtonClickEvent"),
        DataHubUsageEventType.PRODUCT_TOUR_BUTTON_CLICK_EVENT);

    // Test non-existent event type
    assertNull(DataHubUsageEventType.getType("NonExistentEvent"));
  }

  @Test
  public void testEnumValues() {
    // Test that enum values have correct type strings
    assertEquals(
        DataHubUsageEventType.PRODUCT_TOUR_BUTTON_CLICK_EVENT.getType(),
        "ProductTourButtonClickEvent");
    assertEquals(
        DataHubUsageEventType.WELCOME_TO_DATAHUB_MODAL_VIEW_EVENT.getType(),
        "WelcomeToDataHubModalViewEvent");
    assertEquals(
        DataHubUsageEventType.WELCOME_TO_DATAHUB_MODAL_INTERACT_EVENT.getType(),
        "WelcomeToDataHubModalInteractEvent");
    assertEquals(
        DataHubUsageEventType.WELCOME_TO_DATAHUB_MODAL_EXIT_EVENT.getType(),
        "WelcomeToDataHubModalExitEvent");
    assertEquals(
        DataHubUsageEventType.WELCOME_TO_DATAHUB_MODAL_CLICK_VIEW_DOCUMENTATION_EVENT.getType(),
        "WelcomeToDataHubModalClickViewDocumentationEvent");
  }

  @Test
  public void testAllEnumValuesHaveTypes() {
    // Ensure all enum values have non-null type strings
    for (DataHubUsageEventType eventType : DataHubUsageEventType.values()) {
      assertNotNull(eventType.getType(), "Event type should not be null for " + eventType.name());
      // Verify that getType method can find each enum value
      assertEquals(DataHubUsageEventType.getType(eventType.getType()), eventType);
    }
  }
}

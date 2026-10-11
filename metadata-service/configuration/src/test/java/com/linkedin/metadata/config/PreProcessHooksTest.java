package com.linkedin.metadata.config;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import org.testng.annotations.Test;

public class PreProcessHooksTest {

  @Test
  public void testValidate_SyncPathEnabledWithMclConsumer() {
    PreProcessHooks hooks = new PreProcessHooks();
    hooks.setUiEnabled(true);
    hooks.setReprocessEnabled(false);
    PreProcessHooks.validateWhenConsumingMcl(hooks, true);
  }

  @Test
  public void testValidate_ReprocessPathEnabledWithMclConsumer() {
    PreProcessHooks hooks = new PreProcessHooks();
    hooks.setUiEnabled(false);
    hooks.setReprocessEnabled(true);
    PreProcessHooks.validateWhenConsumingMcl(hooks, true);
  }

  @Test
  public void testValidate_BothPathsEnabledWithMclConsumer() {
    PreProcessHooks hooks = new PreProcessHooks();
    hooks.setUiEnabled(true);
    hooks.setReprocessEnabled(true);
    PreProcessHooks.validateWhenConsumingMcl(hooks, true);
  }

  @Test
  public void testValidate_BothPathsDisabledWithoutMclConsumer() {
    PreProcessHooks hooks = new PreProcessHooks();
    hooks.setUiEnabled(false);
    hooks.setReprocessEnabled(false);
    PreProcessHooks.validateWhenConsumingMcl(hooks, false);
    PreProcessHooks.validateWhenConsumingMcl(null, false);
  }

  @Test
  public void testValidate_BothPathsDisabledWithMclConsumerThrows() {
    PreProcessHooks hooks = new PreProcessHooks();
    hooks.setUiEnabled(false);
    hooks.setReprocessEnabled(false);
    IllegalStateException ex =
        expectThrows(
            IllegalStateException.class,
            () -> PreProcessHooks.validateWhenConsumingMcl(hooks, true));
    assertContainsIndexingGuidance(ex);
  }

  @Test
  public void testValidate_NullHooksWithMclConsumerThrows() {
    IllegalStateException ex =
        expectThrows(
            IllegalStateException.class,
            () -> PreProcessHooks.validateWhenConsumingMcl(null, true));
    assertContainsIndexingGuidance(ex);
  }

  @Test
  public void testValidate_DefaultHooksRejectedOnlyWhenConsumingMcl() {
    PreProcessHooks hooks = new PreProcessHooks();
    expectThrows(
        IllegalStateException.class, () -> PreProcessHooks.validateWhenConsumingMcl(hooks, true));
    PreProcessHooks.validateWhenConsumingMcl(hooks, false);
  }

  @Test
  public void testIsMclConsumerEnabled_matchesExactLowercaseTrue() {
    assertTrue(PreProcessHooks.isMclConsumerEnabled("true", "false"));
    assertTrue(PreProcessHooks.isMclConsumerEnabled("false", "true"));
    assertFalse(PreProcessHooks.isMclConsumerEnabled("TRUE", "false"));
    assertFalse(PreProcessHooks.isMclConsumerEnabled("false", "TRUE"));
    assertFalse(PreProcessHooks.isMclConsumerEnabled("false", "false"));
    assertFalse(PreProcessHooks.isMclConsumerEnabled(null, null));
  }

  private static void assertContainsIndexingGuidance(IllegalStateException ex) {
    String message = ex.getMessage();
    assertTrue(message.contains("PRE_PROCESS_HOOKS_UI_ENABLED"), message);
    assertTrue(message.contains("PRE_PROCESS_HOOKS_REPROCESS_ENABLED"), message);
    assertTrue(message.contains("19119"), message);
  }
}

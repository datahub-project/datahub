package com.datahub.authorization.config;

import static org.testng.Assert.*;

import java.util.List;
import org.testng.annotations.Test;

public class ViewUnrestrictedEntityTypesTest {

  @Test
  public void testEmptyDefaults() {
    ViewUnrestrictedEntityTypes config = ViewUnrestrictedEntityTypes.builder().build();
    assertTrue(config.isEmpty());
    assertEquals(config.parsedValue(), List.of());
    assertEquals(config.parsedAdd(), List.of());
    assertEquals(config.parsedRemove(), List.of());
  }

  @Test
  public void testParseCsvTrimsAndLowercases() {
    assertEquals(
        ViewUnrestrictedEntityTypes.parseCsv(" corpuser , CorpGroup,,CONTAINER "),
        List.of("corpuser", "corpgroup", "container"));
  }

  @Test
  public void testParseCsvDedupesPreservingOrder() {
    assertEquals(
        ViewUnrestrictedEntityTypes.parseCsv("corpuser,CorpGroup,corpuser,CONTAINER,corpgroup"),
        List.of("corpuser", "corpgroup", "container"));
  }

  @Test
  public void testValueAddRemoveCanCombine() {
    ViewUnrestrictedEntityTypes config =
        ViewUnrestrictedEntityTypes.builder()
            .value("corpuser,corpGroup,container")
            .add("actionRequest")
            .remove("container")
            .build();
    assertFalse(config.isEmpty());
    assertEquals(config.parsedValue(), List.of("corpuser", "corpgroup", "container"));
    assertEquals(config.parsedAdd(), List.of("actionrequest"));
    assertEquals(config.parsedRemove(), List.of("container"));
  }
}

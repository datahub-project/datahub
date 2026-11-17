package com.linkedin.metadata.search.semantic.profile;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.SemanticRefreshMarker;
import com.linkedin.common.SemanticSourceToken;
import com.linkedin.common.SemanticSourceTokenMap;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticProfilesConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class SemanticProfileRefreshTest {

  @Test
  public void testARecreateOrdersAfterTheDeleteOfTheOldAspect() {
    SemanticSourceToken lastWrite = token(100L, 7L);
    SemanticSourceToken delete = token(100L, 8L);
    SemanticSourceToken recreate = token(500L, 1L);

    assertTrue(SemanticProfileRefresh.compare(delete, lastWrite) > 0);
    assertTrue(SemanticProfileRefresh.compare(recreate, delete) > 0);
    assertEquals(SemanticProfileRefresh.later(delete, recreate), recreate);
  }

  @Test
  public void testAPathChangeMovesOnlyItsOwnClock() {
    ContentBinding body =
        new ContentBinding(java.util.Map.of("documentInfo", Set.of("contents.text")));
    ContentBinding heading = new ContentBinding(java.util.Map.of("documentInfo", Set.of("title")));
    DataMap before = titledBody("same", "heading");
    DataMap after = titledBody("same", "edited");

    assertTrue(
        SemanticProfileRefresh.changedWatchKeys(body, "documentInfo", before, after).isEmpty());
    assertEquals(
        SemanticProfileRefresh.changedWatchKeys(heading, "documentInfo", before, after),
        Set.of("documentInfo#title"));
  }

  @Test
  public void testWatchKeysHaveOneFormPerPath() {
    assertEquals(
        SemanticProfileRefresh.watchKey("documentInfo", "$.contents.text"),
        "documentInfo#contents.text");
    assertEquals(
        SemanticProfileRefresh.watchKey("documentInfo", "contents.text"),
        "documentInfo#contents.text");
    assertEquals(
        SemanticProfileRefresh.watchKeys(
            new ContentBinding(java.util.Map.of("documentInfo", Set.of()))),
        Set.of("documentInfo"));
  }

  @Test
  public void testARestrictedMarkerKeepsOnlyTheProfilesOwnClocks() {
    SemanticSourceTokenMap sources = new SemanticSourceTokenMap();
    sources.put("documentInfo#contents.text", token(10L, 1L));
    sources.put("documentInfo#title", token(20L, 1L));

    SemanticRefreshMarker restricted =
        SemanticProfileRefresh.restrict(
            new SemanticRefreshMarker().setSources(sources), Set.of("documentInfo#title"));

    assertEquals(restricted.getSources().size(), 1);
    assertEquals(restricted.getSources().get("documentInfo#title"), token(20L, 1L));
  }

  @Test
  public void testChangedPathsIgnoreUntouchedFields() {
    DataMap before = documentBody("same");
    DataMap after = documentBody("same");

    assertTrue(
        SemanticProfileRefresh.changedPaths(before, after, Set.of("contents.text")).isEmpty());

    DataMap edited = documentBody("edited");
    assertEquals(
        SemanticProfileRefresh.changedPaths(before, edited, Set.of("contents.text")),
        Set.of("contents.text"));
    assertEquals(
        SemanticProfileRefresh.changedPaths(before, edited, Set.of("$.contents.text")),
        Set.of("$.contents.text"));
  }

  @Test
  public void testAnArrayIndexChangeIsAWatchedChange() {
    DataMap before = items("same", "other");
    DataMap after = items("edited", "other");

    assertEquals(
        SemanticProfileRefresh.changedPaths(before, after, Set.of("items[0].text")),
        Set.of("items[0].text"));
    assertTrue(
        SemanticProfileRefresh.changedPaths(before, after, Set.of("items[1].text")).isEmpty());
    assertTrue(
        SemanticProfileRefresh.changedPaths(before, after, Set.of("items[3].text")).isEmpty());
  }

  @Test
  public void testAWildcardChangeIgnoresSiblingFields() {
    DataMap before = items("same", "other");
    DataMap noteOnly = items("same", "other");
    ((DataMap) ((DataList) noteOnly.get("items")).get(0)).put("note", "added");

    assertTrue(
        SemanticProfileRefresh.changedPaths(before, noteOnly, Set.of("items[*].text")).isEmpty());

    DataMap edited = items("edited", "other");
    assertEquals(
        SemanticProfileRefresh.changedPaths(before, edited, Set.of("items[*].text")),
        Set.of("items[*].text"));

    DataMap removed = items("other");
    assertEquals(
        SemanticProfileRefresh.changedPaths(before, removed, Set.of("items[*].text")),
        Set.of("items[*].text"));
  }

  @Test
  public void testABracketKeySelectsADottedMapKey() {
    DataMap before = new DataMap();
    DataMap texts = new DataMap();
    DataMap entry = new DataMap();
    entry.put("text", "same");
    texts.put("my.key", entry);
    before.put("texts", texts);
    DataMap after = new DataMap();
    DataMap changedTexts = new DataMap();
    DataMap changed = new DataMap();
    changed.put("text", "edited");
    changedTexts.put("my.key", changed);
    after.put("texts", changedTexts);

    assertEquals(
        SemanticProfileRefresh.changedPaths(before, after, Set.of("texts['my.key'].text")),
        Set.of("texts['my.key'].text"));
  }

  @Test
  public void testMapIterationOrderIsNotAChange() {
    DataMap before = texts("a", "one", "b", "two");
    DataMap after = texts("b", "two", "a", "one");

    assertTrue(
        SemanticProfileRefresh.changedPaths(before, after, Set.of("texts[*].text")).isEmpty());
  }

  @Test
  public void testAnUnsupportedPathIsRejectedWhenBindingsLoad() {
    assertThrows(IllegalArgumentException.class, () -> SemanticProfileRefresh.checkPath("$..text"));
    assertThrows(
        IllegalArgumentException.class, () -> SemanticProfileRefresh.checkPath("items[1:2].text"));
    assertThrows(
        IllegalArgumentException.class, () -> SemanticProfileRefresh.checkPath("items[?(@.text)]"));
    assertThrows(
        IllegalArgumentException.class, () -> SemanticProfileRefresh.checkPath("items[0,1].text"));

    SemanticProfilesConfiguration profiles = new SemanticProfilesConfiguration();
    profiles.setBindings(Map.of("bad", Map.of("notes", List.of("$..text"))));
    SemanticSearchConfiguration configuration = new SemanticSearchConfiguration();
    configuration.setProfiles(profiles);
    assertThrows(
        IllegalArgumentException.class, () -> new ConfiguredSemanticProfileRegistry(configuration));

    profiles.setBindings(Map.of("custom", Map.of("notes", List.of("texts['my.key'].text"))));
    ModelEmbeddingConfig model = new ModelEmbeddingConfig();
    model.setBinding("custom");
    configuration.setModels(Map.of("profile", model));
    ContentBinding binding =
        new ConfiguredSemanticProfileRegistry(configuration).bindingFor("document", "profile");
    assertTrue(binding.aspectFields().get("notes").contains("texts['my.key'].text"));
    new ConfiguredSemanticProfileRegistry(null);
  }

  @Test
  public void testAnUnknownBindingNameFailsStartup() {
    SemanticProfilesConfiguration profiles = new SemanticProfilesConfiguration();
    profiles.setDefaultBinding("documentTxt");
    SemanticSearchConfiguration configuration = new SemanticSearchConfiguration();
    configuration.setProfiles(profiles);
    assertThrows(
        IllegalArgumentException.class, () -> new ConfiguredSemanticProfileRegistry(configuration));

    profiles.setDefaultBinding(null);
    ModelEmbeddingConfig model = new ModelEmbeddingConfig();
    model.setBinding("missing");
    configuration.setModels(Map.of("profile", model));
    assertThrows(
        IllegalArgumentException.class, () -> new ConfiguredSemanticProfileRegistry(configuration));

    model.setBinding("external");
    ConfiguredSemanticProfileRegistry registry =
        new ConfiguredSemanticProfileRegistry(configuration);
    assertTrue(registry.bindingFor("document", "profile").isExternal());
    assertFalse(registry.bindingFor("document", "undeclared").isExternal());
  }

  private static SemanticSourceToken token(long createdTime, long version) {
    return new SemanticSourceToken().setCreatedTime(createdTime).setVersion(version);
  }

  private static DataMap titledBody(String text, String title) {
    DataMap body = documentBody(text);
    body.put("title", title);
    return body;
  }

  private static DataMap documentBody(String text) {
    DataMap contents = new DataMap();
    contents.put("text", text);
    DataMap body = new DataMap();
    body.put("contents", contents);
    return body;
  }

  private static DataMap items(String... texts) {
    DataList list = new DataList();
    for (String text : texts) {
      DataMap item = new DataMap();
      item.put("text", text);
      list.add(item);
    }
    DataMap body = new DataMap();
    body.put("items", list);
    return body;
  }

  private static DataMap texts(String... keysAndValues) {
    DataMap map = new DataMap();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      DataMap entry = new DataMap();
      entry.put("text", keysAndValues[i + 1]);
      map.put(keysAndValues[i], entry);
    }
    DataMap body = new DataMap();
    body.put("texts", map);
    return body;
  }
}

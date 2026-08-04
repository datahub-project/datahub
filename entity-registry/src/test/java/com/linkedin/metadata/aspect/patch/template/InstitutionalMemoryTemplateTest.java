package com.linkedin.metadata.aspect.patch.template;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.InstitutionalMemoryMetadataArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.template.common.InstitutionalMemoryTemplate;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class InstitutionalMemoryTemplateTest {

  private static final InstitutionalMemoryTemplate TEMPLATE = new InstitutionalMemoryTemplate();

  private static InstitutionalMemoryMetadata element(String url, String description) {
    return new InstitutionalMemoryMetadata()
        .setUrl(new Url(url))
        .setDescription(description)
        .setCreateStamp(
            new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:datahub")).setTime(0L));
  }

  private static JsonPatch addElement(String url, String description) {
    return Json.createPatch(
        Json.createArrayBuilder()
            .add(
                Json.createObjectBuilder()
                    .add("op", "add")
                    .add("path", "/elements/" + url)
                    .add(
                        "value",
                        Json.createObjectBuilder()
                            .add("url", url)
                            .add("description", description)
                            .add(
                                "createStamp",
                                Json.createObjectBuilder()
                                    .add("actor", "urn:li:corpuser:datahub")
                                    .add("time", 0))))
            .build());
  }

  @Test
  public void testTwoWritersBothSurvive() throws Exception {
    // The reason this template is worth having: institutionalMemory is an
    // append-shaped aspect, and without patch support the only way to add a link
    // is read-modify-write, which drops whatever another writer added in between.
    InstitutionalMemory initial = new InstitutionalMemory();
    initial.setElements(new InstitutionalMemoryMetadataArray());

    InstitutionalMemory afterFirst =
        TEMPLATE.applyPatch(initial, addElement("https://example.org/a", "first writer"));
    InstitutionalMemory result =
        TEMPLATE.applyPatch(afterFirst, addElement("https://example.org/b", "second writer"));

    Assert.assertNotNull(result.getElements());
    Assert.assertEquals(result.getElements().size(), 2);
    List<String> urls = result.getElements().stream().map(e -> e.getUrl().toString()).toList();
    Assert.assertTrue(urls.contains("https://example.org/a"), "first writer's link should survive");
    Assert.assertTrue(
        urls.contains("https://example.org/b"), "second writer's link should be added");
  }

  @Test
  public void testAddOnSameUrlUpserts() throws Exception {
    InstitutionalMemory initial = new InstitutionalMemory();
    initial.setElements(
        new InstitutionalMemoryMetadataArray(element("https://example.org/a", "before")));

    InstitutionalMemory result =
        TEMPLATE.applyPatch(initial, addElement("https://example.org/a", "after"));

    Assert.assertNotNull(result.getElements());
    Assert.assertEquals(result.getElements().size(), 1, "url is the key, so this is an update");
    Assert.assertEquals(result.getElements().get(0).getDescription(), "after");
  }

  @Test
  public void testRemoveOneOfTwoEntries() throws Exception {
    InstitutionalMemory initial = new InstitutionalMemory();
    initial.setElements(
        new InstitutionalMemoryMetadataArray(
            element("https://example.org/a", "a"), element("https://example.org/b", "b")));

    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "remove")
                        .add("path", "/elements/https://example.org/a"))
                .build());

    InstitutionalMemory result = TEMPLATE.applyPatch(initial, patch);

    Assert.assertNotNull(result.getElements());
    Assert.assertEquals(result.getElements().size(), 1);
    Assert.assertEquals(result.getElements().get(0).getUrl().toString(), "https://example.org/b");
  }

  @Test
  public void testDefaultIsEmptyNotNull() {
    InstitutionalMemory defaultValue = TEMPLATE.getDefault();
    Assert.assertNotNull(defaultValue.getElements());
    Assert.assertTrue(defaultValue.getElements().isEmpty());
  }
}

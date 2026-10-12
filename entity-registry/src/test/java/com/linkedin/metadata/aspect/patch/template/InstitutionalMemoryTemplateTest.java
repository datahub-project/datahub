package com.linkedin.metadata.aspect.patch.template;

import static com.linkedin.metadata.Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.InstitutionalMemoryMetadataArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.common.InstitutionalMemoryTemplate;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class InstitutionalMemoryTemplateTest {

  private static final InstitutionalMemoryTemplate TEMPLATE = new InstitutionalMemoryTemplate();
  private static final String ACTOR = "urn:li:corpuser:datahub";

  @BeforeClass
  public static void disableSchemaAnnotationAssertions() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  private static InstitutionalMemoryMetadata element(String url, String description, long time) {
    return new InstitutionalMemoryMetadata()
        .setUrl(new Url(url))
        .setDescription(description)
        .setCreateStamp(new AuditStamp().setActor(UrnUtils.getUrn(ACTOR)).setTime(time));
  }

  private static String escape(String value) {
    return value.replace("~", "~0").replace("/", "~1");
  }

  private static String pointer(String url, String description) {
    return "/elements/" + escape(url) + "/" + escape(description);
  }

  private static JsonPatch addElement(String url, String description, long time) {
    return Json.createPatch(
        Json.createArrayBuilder()
            .add(
                Json.createObjectBuilder()
                    .add("op", "add")
                    .add("path", pointer(url, description))
                    .add(
                        "value",
                        Json.createArrayBuilder()
                            .add(
                                Json.createObjectBuilder()
                                    .add("url", url)
                                    .add("description", description)
                                    .add(
                                        "createStamp",
                                        Json.createObjectBuilder()
                                            .add("actor", ACTOR)
                                            .add("time", time)))))
            .build());
  }

  private static InstitutionalMemory empty() {
    InstitutionalMemory institutionalMemory = new InstitutionalMemory();
    institutionalMemory.setElements(new InstitutionalMemoryMetadataArray());
    return institutionalMemory;
  }

  @Test
  public void testTwoWritersBothSurvive() throws Exception {
    InstitutionalMemory afterFirst =
        TEMPLATE.applyPatch(empty(), addElement("https://example.org/a", "first", 1L));
    InstitutionalMemory result =
        TEMPLATE.applyPatch(afterFirst, addElement("https://example.org/b", "second", 2L));

    Assert.assertEquals(result.getElements().size(), 2);
    List<String> urls = result.getElements().stream().map(e -> e.getUrl().toString()).toList();
    Assert.assertTrue(urls.contains("https://example.org/a"));
    Assert.assertTrue(urls.contains("https://example.org/b"));
  }

  @Test
  public void testSameUrlDifferentDescriptionsBothSurvive() throws Exception {
    InstitutionalMemory initial = new InstitutionalMemory();
    initial.setElements(
        new InstitutionalMemoryMetadataArray(
            element("https://example.org/a", "design", 1L),
            element("https://example.org/a", "runbook", 2L)));

    InstitutionalMemory result =
        TEMPLATE.applyPatch(initial, addElement("https://example.org/b", "other", 3L));

    Assert.assertEquals(result.getElements().size(), 3);
    List<String> descriptions =
        result.getElements().stream()
            .filter(e -> e.getUrl().toString().equals("https://example.org/a"))
            .map(InstitutionalMemoryMetadata::getDescription)
            .toList();
    Assert.assertTrue(descriptions.contains("design"));
    Assert.assertTrue(descriptions.contains("runbook"));
  }

  @Test
  public void testAddOnSameUrlAndDescriptionUpserts() throws Exception {
    InstitutionalMemory initial = new InstitutionalMemory();
    initial.setElements(
        new InstitutionalMemoryMetadataArray(element("https://example.org/a", "design", 1L)));

    InstitutionalMemory result =
        TEMPLATE.applyPatch(initial, addElement("https://example.org/a", "design", 9L));

    Assert.assertEquals(result.getElements().size(), 1);
    Assert.assertEquals(result.getElements().get(0).getDescription(), "design");
    Assert.assertEquals(result.getElements().get(0).getCreateStamp().getTime().longValue(), 9L);
  }

  @Test
  public void testRemoveOnePairLeavesSiblingLabelAndOtherUrl() throws Exception {
    InstitutionalMemory initial = new InstitutionalMemory();
    initial.setElements(
        new InstitutionalMemoryMetadataArray(
            element("https://example.org/a", "design", 1L),
            element("https://example.org/a", "runbook", 2L),
            element("https://example.org/b", "other", 3L)));

    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "remove")
                        .add("path", pointer("https://example.org/a", "design")))
                .build());

    InstitutionalMemory result = TEMPLATE.applyPatch(initial, patch);

    Assert.assertEquals(result.getElements().size(), 2);
    List<String> keys =
        result.getElements().stream()
            .map(e -> e.getUrl().toString() + " " + e.getDescription())
            .toList();
    Assert.assertFalse(keys.contains("https://example.org/a design"));
    Assert.assertTrue(keys.contains("https://example.org/a runbook"));
    Assert.assertTrue(keys.contains("https://example.org/b other"));
  }

  @Test
  public void testRemoveUrlDropsEveryLabelForThatUrl() throws Exception {
    InstitutionalMemory initial = new InstitutionalMemory();
    initial.setElements(
        new InstitutionalMemoryMetadataArray(
            element("https://example.org/a", "design", 1L),
            element("https://example.org/a", "runbook", 2L),
            element("https://example.org/b", "other", 3L)));

    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "remove")
                        .add("path", "/elements/" + escape("https://example.org/a")))
                .build());

    InstitutionalMemory result = TEMPLATE.applyPatch(initial, patch);

    Assert.assertEquals(result.getElements().size(), 1);
    Assert.assertEquals(result.getElements().get(0).getUrl().toString(), "https://example.org/b");
  }

  @Test
  public void testSlashInUrlIsJsonPointerEscaped() throws Exception {
    String url = "https://example.org/a";
    Assert.assertEquals(pointer(url, "design"), "/elements/https:~1~1example.org~1a/design");

    InstitutionalMemory result = TEMPLATE.applyPatch(empty(), addElement(url, "design", 1L));

    Assert.assertEquals(result.getElements().size(), 1);
    Assert.assertEquals(result.getElements().get(0).getUrl().toString(), url);
    Assert.assertEquals(result.getElements().get(0).getDescription(), "design");
  }

  @Test
  public void testDefaultIsEmptyNotNull() {
    InstitutionalMemory defaultValue = TEMPLATE.getDefault();
    Assert.assertNotNull(defaultValue.getElements());
    Assert.assertTrue(defaultValue.getElements().isEmpty());
  }

  @Test
  public void testRegistryRegistersTemplate() {
    RecordTemplate defaultValue =
        SnapshotEntityRegistry.getInstance()
            .getAspectTemplateEngine()
            .getDefaultTemplate(INSTITUTIONAL_MEMORY_ASPECT_NAME);
    Assert.assertTrue(defaultValue instanceof InstitutionalMemory);
    Assert.assertNotNull(((InstitutionalMemory) defaultValue).getElements());
    Assert.assertTrue(((InstitutionalMemory) defaultValue).getElements().isEmpty());
  }
}

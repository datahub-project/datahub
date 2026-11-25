package com.linkedin.metadata.aspect.patch.template;

import static com.linkedin.metadata.Constants.APPLICATION_PROPERTIES_ASPECT_NAME;

import com.linkedin.application.ApplicationProperties;
import com.linkedin.metadata.aspect.patch.template.application.ApplicationPropertiesTemplate;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ApplicationPropertiesTemplateTest {

  /**
   * Without a registered template the engine returns no default aspect, which makes PATCH fail
   * outright for applications that have no applicationProperties yet.
   */
  @Test
  public void testTemplateIsRegistered() {
    Assert.assertNotNull(
        SnapshotEntityRegistry.getInstance()
            .getAspectTemplateEngine()
            .getDefaultTemplate(APPLICATION_PROPERTIES_ASPECT_NAME));
  }

  @Test
  public void testPatchSetsFields() throws Exception {
    ApplicationPropertiesTemplate template = new ApplicationPropertiesTemplate();

    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/name")
                        .add("value", "My Application"))
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/description")
                        .add("value", "Some description"))
                .build());

    ApplicationProperties result = template.applyPatch(template.getDefault(), patch);

    Assert.assertEquals(result.getName(), "My Application");
    Assert.assertEquals(result.getDescription(), "Some description");
  }

  @Test
  public void testPatchLeavesUntouchedFieldsIntact() throws Exception {
    ApplicationPropertiesTemplate template = new ApplicationPropertiesTemplate();
    ApplicationProperties initial =
        new ApplicationProperties().setName("Original").setDescription("Original description");

    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/description")
                        .add("value", "Updated description"))
                .build());

    ApplicationProperties result = template.applyPatch(initial, patch);

    Assert.assertEquals(result.getName(), "Original");
    Assert.assertEquals(result.getDescription(), "Updated description");
  }
}

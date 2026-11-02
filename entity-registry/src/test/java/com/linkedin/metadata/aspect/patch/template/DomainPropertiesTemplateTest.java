package com.linkedin.metadata.aspect.patch.template;

import static com.linkedin.metadata.Constants.DOMAIN_PROPERTIES_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.domain.DomainProperties;
import com.linkedin.metadata.aspect.patch.template.domain.DomainPropertiesTemplate;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DomainPropertiesTemplateTest {

  /** Without a registered template, PATCH on an existing domainProperties aspect fails. */
  @Test
  public void testTemplateIsRegistered() {
    Assert.assertNotNull(
        SnapshotEntityRegistry.getInstance()
            .getAspectTemplateEngine()
            .getDefaultTemplate(DOMAIN_PROPERTIES_ASPECT_NAME));
  }

  @Test
  public void testPatchLeavesUntouchedFieldsIntact() throws Exception {
    DomainPropertiesTemplate template = new DomainPropertiesTemplate();
    DomainProperties initial =
        new DomainProperties()
            .setName("Marketing")
            .setDescription("Original description")
            .setParentDomain(Urn.createFromString("urn:li:domain:parent"));

    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/description")
                        .add("value", "Updated description"))
                .build());

    DomainProperties result = template.applyPatch(initial, patch);

    Assert.assertEquals(result.getName(), "Marketing");
    Assert.assertEquals(result.getDescription(), "Updated description");
    Assert.assertEquals(result.getParentDomain().toString(), "urn:li:domain:parent");
  }

  @Test
  public void testPatchRemovesParentDomain() throws Exception {
    DomainPropertiesTemplate template = new DomainPropertiesTemplate();
    DomainProperties initial =
        new DomainProperties()
            .setName("Campaigns")
            .setParentDomain(Urn.createFromString("urn:li:domain:parent"));

    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(Json.createObjectBuilder().add("op", "remove").add("path", "/parentDomain"))
                .build());

    DomainProperties result = template.applyPatch(initial, patch);

    Assert.assertEquals(result.getName(), "Campaigns");
    Assert.assertFalse(result.hasParentDomain());
  }
}

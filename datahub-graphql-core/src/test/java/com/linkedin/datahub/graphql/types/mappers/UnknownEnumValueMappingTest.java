package com.linkedin.datahub.graphql.types.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.common.Origin;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.Form;
import com.linkedin.datahub.graphql.generated.FormPromptType;
import com.linkedin.datahub.graphql.generated.OriginType;
import com.linkedin.datahub.graphql.types.form.FormMapper;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormPrompt;
import com.linkedin.form.FormPromptArray;
import com.linkedin.metadata.Constants;
import org.testng.annotations.Test;

/**
 * Enum values a newer version wrote (read after a rollback) arrive as $UNKNOWN; mappers fall back
 * or skip the item instead of failing the whole entity or batch.
 */
public class UnknownEnumValueMappingTest {

  private static final String NEWER_VALUE = "VALUE_FROM_NEWER_BUILD";

  @Test
  public void testFormPromptOfUnknownTypeIsSkipped() {
    Urn formUrn = UrnUtils.getUrn("urn:li:form:rollback");
    FormPrompt known =
        new FormPrompt()
            .setId("known")
            .setTitle("Known")
            .setType(com.linkedin.form.FormPromptType.STRUCTURED_PROPERTY)
            .setRequired(true);
    FormPrompt unknown = new FormPrompt().setId("unknown").setTitle("Unknown").setRequired(true);
    unknown.data().put("type", NEWER_VALUE);
    FormInfo info =
        new FormInfo()
            .setName("Form")
            .setType(com.linkedin.form.FormType.COMPLETION)
            .setPrompts(new FormPromptArray(known, unknown));
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.FORM_INFO_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(info.data())));

    Form form = FormMapper.map(null, new EntityResponse().setUrn(formUrn).setAspects(aspects));

    assertEquals(form.getInfo().getPrompts().size(), 1);
    assertEquals(form.getInfo().getPrompts().get(0).getType(), FormPromptType.STRUCTURED_PROPERTY);
  }

  @Test
  public void testMapDefaultNullForUnknownValue() {
    Origin origin = new Origin();
    origin.data().put("type", NEWER_VALUE);

    assertNull(PdlEnumMapper.mapDefaultNull(OriginType.class, origin.getType()));
  }

  @Test
  public void testOwnerOfUnknownEntityTypeIsSkippedAndUnknownOwnershipTypeFallsBack() {
    com.linkedin.common.Owner user =
        new com.linkedin.common.Owner()
            .setOwner(UrnUtils.getUrn("urn:li:corpuser:alice"))
            .setType(com.linkedin.common.OwnershipType.TECHNICAL_OWNER);
    user.data().put("type", NEWER_VALUE);
    com.linkedin.common.Owner unknownOwner =
        new com.linkedin.common.Owner()
            .setOwner(UrnUtils.getUrn("urn:li:entityFromNewerBuild:x"))
            .setType(com.linkedin.common.OwnershipType.TECHNICAL_OWNER);
    com.linkedin.common.Ownership ownership =
        new com.linkedin.common.Ownership()
            .setOwners(new com.linkedin.common.OwnerArray(user, unknownOwner))
            .setLastModified(
                new com.linkedin.common.AuditStamp()
                    .setTime(0L)
                    .setActor(UrnUtils.getUrn("urn:li:corpuser:alice")));

    var mapped =
        com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper.map(
            com.linkedin.datahub.graphql.TestUtils.getMockAllowContext(),
            ownership,
            UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,t,PROD)"));

    assertEquals(mapped.getOwners().size(), 1);
    assertEquals(
        mapped.getOwners().get(0).getType(),
        com.linkedin.datahub.graphql.generated.OwnershipType.CUSTOM);
  }

  @Test
  public void testAttributionWithUnknownActorIsLeftOut() {
    com.linkedin.common.MetadataAttribution attribution =
        new com.linkedin.common.MetadataAttribution()
            .setTime(0L)
            .setActor(UrnUtils.getUrn("urn:li:entityFromNewerBuild:x"));

    assertNull(
        com.linkedin.datahub.graphql.types.common.mappers.MetadataAttributionMapper.map(
            null, attribution));
  }

  @Test
  public void testAnalyticsCellForUnknownEntityTypeHasNoLink() {
    var cell =
        com.linkedin.datahub.graphql.analytics.service.AnalyticsUtil.buildCellWithEntityLandingPage(
            "urn:li:entityFromNewerBuild:x");

    assertNull(cell.getLinkParams());
    assertEquals(cell.getValue(), "urn:li:entityFromNewerBuild:x");
  }
}

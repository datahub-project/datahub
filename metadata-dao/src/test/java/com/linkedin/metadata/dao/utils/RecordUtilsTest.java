package com.linkedin.metadata.dao.utils;

import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.validator.InvalidSchemaException;
import com.linkedin.metadata.validator.ValidationUtils;
import java.io.IOException;
import org.testng.annotations.Test;

import static com.linkedin.metadata.utils.TestUtils.*;
import static org.testng.Assert.*;


public class RecordUtilsTest {

  @Test
  public void testToJsonString() throws IOException {
    Ownership ownership = makeOwnership("foo");
    String expected =
        loadJsonFromResource("ownership.json").replaceAll("\\s+", "").replaceAll("\\n", "").replaceAll("\\r", "");

    String actual = RecordUtils.toJsonString(ownership);

    assertEquals(actual, expected);
  }

  @Test
  public void testToRecordTemplate() throws IOException {
    Ownership expected = makeOwnership("foo");
    String jsonString = loadJsonFromResource("ownership.json");

    Ownership actual = RecordUtils.toRecordTemplate(Ownership.class, jsonString);

    assertEquals(actual, expected);

    RecordTemplate actual2 = RecordUtils.toRecordTemplate("com.linkedin.common.Ownership", expected.data());

    assertEquals(actual2.getClass(), Ownership.class);
    assertEquals(actual2, expected);
  }

  @Test(expectedExceptions = ModelConversionException.class)
  public void testToRecordTemplateFromInvalidString() {
    RecordUtils.toRecordTemplate(Ownership.class, "invalid_json");
  }

  @Test
  public void testGetValidRecordDataSchemaField() {
    RecordDataSchema schema = ValidationUtils.getRecordSchema(Ownership.class);
    RecordDataSchema.Field expected = schema.getField("owners");

    assertEquals(RecordUtils.getRecordDataSchemaField(makeOwnership("foo"), "owners"), expected);
  }

  @Test(expectedExceptions = InvalidSchemaException.class)
  public void testGetInvalidRecordDataSchemaField() {
    RecordUtils.getRecordDataSchemaField(makeOwnership("foo"), "non-existing-field");
  }

  @Test
  public void testSetRecordTemplatePrimitiveField() {
    CorpUserInfo corpUserInfo = new CorpUserInfo();

    RecordUtils.setRecordTemplatePrimitiveField(corpUserInfo, "active", Boolean.FALSE);
    RecordUtils.setRecordTemplatePrimitiveField(corpUserInfo, "email", "foo@bar.com");
    RecordUtils.setRecordTemplatePrimitiveField(corpUserInfo, "departmentId", Long.valueOf(1234L));

    assertFalse(corpUserInfo.isActive());
    assertEquals(corpUserInfo.getEmail(), "foo@bar.com");
    assertEquals(corpUserInfo.getDepartmentId(), Long.valueOf(1234L));
  }

  @Test
  public void testSetRecordTemplateComplexField() throws IOException {
    Ownership ownership = loadOwnership("ownership.json");
    OwnerArray owners = makeOwnership("bar").getOwners();

    RecordUtils.setRecordTemplateComplexField(ownership, "owners", owners);

    assertEquals(ownership.getOwners().get(0).getOwner(), new CorpuserUrn("bar"));
  }

  @Test
  public void testGetRecordTemplatePrimitiveField() throws IOException {
    CorpUserInfo corpUserInfo = loadCorpUserInfo("corp-user-info.json");

    assertTrue(RecordUtils.getRecordTemplateField(corpUserInfo, "active", Boolean.class));
    assertEquals(RecordUtils.getRecordTemplateField(corpUserInfo, "email", String.class), "foo@bar.com");
    assertEquals(RecordUtils.getRecordTemplateField(corpUserInfo, "departmentId", Long.class), Long.valueOf(1234L));
  }

  @Test
  public void testGetRecordTemplateWrappedField() throws IOException {
    Ownership ownership = loadOwnership("ownership.json");

    OwnerArray owners = RecordUtils.getRecordTemplateWrappedField(ownership, "owners", OwnerArray.class);

    assertEquals(owners.get(0).getOwner(), new CorpuserUrn("foo"));
  }

  @Test(expectedExceptions = InvalidSchemaException.class)
  public void testGetInvalidMetadataSnapshotClassFromName() {
    ModelUtils.getMetadataSnapshotClassFromName("com.linkedin.common.Ownership");
  }

  private Ownership loadOwnership(String resourceName) throws IOException {
    return RecordUtils.toRecordTemplate(Ownership.class, loadJsonFromResource(resourceName));
  }

  private CorpUserInfo loadCorpUserInfo(String resourceName) throws IOException {
    return RecordUtils.toRecordTemplate(CorpUserInfo.class, loadJsonFromResource(resourceName));
  }
}

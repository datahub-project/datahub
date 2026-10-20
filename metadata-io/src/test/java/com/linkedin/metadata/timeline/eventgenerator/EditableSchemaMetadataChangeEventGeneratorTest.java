package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.utils.SchemaFieldUtils.generateSchemaFieldUrn;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

public class EditableSchemaMetadataChangeEventGeneratorTest
    extends AbstractTestNGSpringContextTests {

  private static Urn getTestUrn() throws URISyntaxException {
    return Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
  }

  private static AuditStamp getTestAuditStamp() throws URISyntaxException {
    return new AuditStamp()
        .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
        .setTime(1683829509553L);
  }

  private static Aspect<EditableSchemaMetadata> getEditableSchemaMetadata(
      String fieldPath, String description) {
    return new Aspect<>(
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    List.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(fieldPath)
                            .setDescription(description)))),
        new SystemMetadata());
  }

  @Test
  public void testDocumentationChangeCarriesPreviousValue() throws Exception {
    // Editing a column's documentation in the UI writes here rather than to schemaMetadata. The
    // schema history UI renders documentation as a diff between the previous and the new value, so
    // a MODIFY carrying only the new value cannot be displayed at all.
    EditableSchemaMetadataChangeEventGenerator test =
        new EditableSchemaMetadataChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "editableSchemaMetadata";
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<EditableSchemaMetadata> from = getEditableSchemaMetadata("id", "old doc");
    Aspect<EditableSchemaMetadata> to = getEditableSchemaMetadata("id", "new doc");

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);
    assertEquals(1, actual.size());
    Map<String, Object> parameters = actual.get(0).getParameters();
    assertEquals(parameters.get("description"), "new doc");
    assertEquals(parameters.get("previousDescription"), "old doc");
  }

  @Test
  public void testEditedPrimaryKeyChangesEmitEvents() throws Exception {
    EditableSchemaMetadataChangeEventGenerator test =
        new EditableSchemaMetadataChangeEventGenerator();
    Aspect<EditableSchemaMetadata> from = getEditableSchemaMetadata("id", "doc");
    from.getValue().setPrimaryKeys(new StringArray(List.of("id")));
    Aspect<EditableSchemaMetadata> to = getEditableSchemaMetadata("id", "doc");
    to.getValue().setPrimaryKeys(new StringArray(List.of("col_a")));

    List<ChangeEvent> actual =
        test.getChangeEvents(
            getTestUrn(), "dataset", "editableSchemaMetadata", from, to, getTestAuditStamp());

    assertEquals(2, actual.size());
    assertEquals(
        Set.of(
            generateSchemaFieldUrn(getTestUrn(), "id").toString(),
            generateSchemaFieldUrn(getTestUrn(), "col_a").toString()),
        actual.stream().map(ChangeEvent::getModifier).collect(Collectors.toSet()));
    actual.forEach(event -> assertEquals(ChangeCategory.TECHNICAL_SCHEMA, event.getCategory()));
  }
}

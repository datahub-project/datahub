package com.linkedin.datahub.graphql.types.dataset.mappers;

import static org.testng.Assert.assertEquals;

import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.DatasetUpdateInput;
import com.linkedin.datahub.graphql.generated.EditableSchemaMetadataUpdate;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaMetadata;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.testng.annotations.Test;

public class DatasetUpdateInputMapperTest {

  @Test
  public void testMapsEditablePrimaryKeys() {
    EditableSchemaMetadataUpdate update = new EditableSchemaMetadataUpdate();
    update.setEditableSchemaFieldInfo(List.of());
    update.setPrimaryKeys(List.of("id", "as_of_date"));
    DatasetUpdateInput input = new DatasetUpdateInput();
    input.setEditableSchemaMetadata(update);

    MetadataChangeProposal proposal =
        DatasetUpdateInputMapper.map(null, input, UrnUtils.getUrn("urn:li:corpuser:test")).stream()
            .filter(p -> Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME.equals(p.getAspectName()))
            .findFirst()
            .orElseThrow();
    EditableSchemaMetadata aspect =
        RecordUtils.toRecordTemplate(
            EditableSchemaMetadata.class,
            proposal.getAspect().getValue().asString(StandardCharsets.UTF_8));

    assertEquals(aspect.getPrimaryKeys(), List.of("id", "as_of_date"));
  }
}

package com.linkedin.datahub.graphql.types.dataset.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import java.util.List;
import org.testng.annotations.Test;

public class EditableSchemaMetadataMapperTest {

  private static final Urn DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:s3,my_bucket/events,PROD)");

  @Test
  public void testMapsPrimaryKeys() {
    EditableSchemaMetadata input =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(new EditableSchemaFieldInfoArray())
            .setPrimaryKeys(new StringArray(List.of("id", "as_of_date")));

    assertEquals(
        EditableSchemaMetadataMapper.map(null, input, DATASET).getPrimaryKeys(),
        List.of("id", "as_of_date"));
  }

  @Test
  public void testPrimaryKeysAbsentWhenNotSet() {
    EditableSchemaMetadata input =
        new EditableSchemaMetadata().setEditableSchemaFieldInfo(new EditableSchemaFieldInfoArray());

    assertNull(EditableSchemaMetadataMapper.map(null, input, DATASET).getPrimaryKeys());
  }
}

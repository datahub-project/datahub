/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.schema.EditableSchemaMetadata;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class EditableSchemaMetadataMapper {

  public static final EditableSchemaMetadataMapper INSTANCE = new EditableSchemaMetadataMapper();

  public static com.linkedin.datahub.graphql.generated.EditableSchemaMetadata map(
      @Nullable QueryContext context,
      @Nonnull final EditableSchemaMetadata metadata,
      @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(context, metadata, entityUrn);
  }

  public com.linkedin.datahub.graphql.generated.EditableSchemaMetadata apply(
      @Nullable QueryContext context,
      @Nonnull final EditableSchemaMetadata input,
      @Nonnull final Urn entityUrn) {
    final com.linkedin.datahub.graphql.generated.EditableSchemaMetadata result =
        new com.linkedin.datahub.graphql.generated.EditableSchemaMetadata();
    result.setEditableSchemaFieldInfo(
        input.getEditableSchemaFieldInfo().stream()
            .map(schemaField -> EditableSchemaFieldInfoMapper.map(context, schemaField, entityUrn))
            .collect(Collectors.toList()));
    return result;
  }
}

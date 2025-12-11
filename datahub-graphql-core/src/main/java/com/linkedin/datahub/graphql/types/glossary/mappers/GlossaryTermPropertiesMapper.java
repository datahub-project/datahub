/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.glossary.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.GlossaryTermProperties;
import com.linkedin.datahub.graphql.generated.ResolvedAuditStamp;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import javax.annotation.Nonnull;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class GlossaryTermPropertiesMapper {

  public static final GlossaryTermPropertiesMapper INSTANCE = new GlossaryTermPropertiesMapper();

  public static GlossaryTermProperties map(
      @Nonnull final com.linkedin.glossary.GlossaryTermInfo glossaryTermInfo,
      Urn entityUrn,
      final ResolvedAuditStamp createdAuditStamp) {
    return INSTANCE.apply(glossaryTermInfo, entityUrn, createdAuditStamp);
  }

  public GlossaryTermProperties apply(
      @Nonnull final com.linkedin.glossary.GlossaryTermInfo glossaryTermInfo,
      Urn entityUrn,
      final ResolvedAuditStamp createdAuditStamp) {
    com.linkedin.datahub.graphql.generated.GlossaryTermProperties result =
        new com.linkedin.datahub.graphql.generated.GlossaryTermProperties();
    result.setDefinition(glossaryTermInfo.getDefinition());
    result.setDescription(glossaryTermInfo.getDefinition());
    result.setTermSource(glossaryTermInfo.getTermSource());
    if (glossaryTermInfo.hasName()) {
      result.setName(glossaryTermInfo.getName());
    }
    if (glossaryTermInfo.hasSourceRef()) {
      result.setSourceRef(glossaryTermInfo.getSourceRef());
    }
    if (glossaryTermInfo.hasSourceUrl()) {
      result.setSourceUrl(glossaryTermInfo.getSourceUrl().toString());
    }
    if (glossaryTermInfo.hasCustomProperties()) {
      result.setCustomProperties(
          CustomPropertiesMapper.map(glossaryTermInfo.getCustomProperties(), entityUrn));
    }
    result.setCreatedOn(createdAuditStamp);
    return result;
  }
}

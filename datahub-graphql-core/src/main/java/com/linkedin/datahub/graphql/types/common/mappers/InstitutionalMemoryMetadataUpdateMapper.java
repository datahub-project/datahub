/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.url.Url;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.InstitutionalMemoryMetadataUpdate;
import com.linkedin.datahub.graphql.types.corpuser.CorpUserUtils;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class InstitutionalMemoryMetadataUpdateMapper
    implements ModelMapper<InstitutionalMemoryMetadataUpdate, InstitutionalMemoryMetadata> {

  private static final InstitutionalMemoryMetadataUpdateMapper INSTANCE =
      new InstitutionalMemoryMetadataUpdateMapper();

  public static InstitutionalMemoryMetadata map(
      @Nullable QueryContext context, @Nonnull final InstitutionalMemoryMetadataUpdate input) {
    return INSTANCE.apply(context, input);
  }

  @Override
  public InstitutionalMemoryMetadata apply(
      @Nullable QueryContext context, @Nonnull final InstitutionalMemoryMetadataUpdate input) {
    final InstitutionalMemoryMetadata metadata = new InstitutionalMemoryMetadata();
    metadata.setDescription(input.getDescription());
    metadata.setUrl(new Url(input.getUrl()));
    metadata.setCreateStamp(
        new AuditStamp()
            .setActor(CorpUserUtils.getCorpUserUrn(input.getAuthor()))
            .setTime(
                input.getCreatedAt() == null ? System.currentTimeMillis() : input.getCreatedAt()));
    return metadata;
  }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.InstitutionalMemoryMetadata;
import com.linkedin.datahub.graphql.generated.InstitutionalMemoryMetadataSettings;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class InstitutionalMemoryMetadataMapper {

  public static final InstitutionalMemoryMetadataMapper INSTANCE =
      new InstitutionalMemoryMetadataMapper();

  public static InstitutionalMemoryMetadata map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.InstitutionalMemoryMetadata metadata,
      @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(context, metadata, entityUrn);
  }

  public InstitutionalMemoryMetadata apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.InstitutionalMemoryMetadata input,
      @Nonnull final Urn entityUrn) {
    final InstitutionalMemoryMetadata result = new InstitutionalMemoryMetadata();
    result.setUrl(input.getUrl().toString());
    result.setDescription(input.getDescription()); // deprecated field
    result.setLabel(input.getDescription());
    result.setAuthor(getAuthor(input.getCreateStamp().getActor().toString()));
    result.setActor(ResolvedActorMapper.map(input.getCreateStamp().getActor()));
    result.setCreated(AuditStampMapper.map(context, input.getCreateStamp()));
    if (input.getUpdateStamp() != null) {
      result.setUpdated(AuditStampMapper.map(context, input.getUpdateStamp()));
    }
    result.setAssociatedUrn(entityUrn.toString());
    if (input.getSettings() != null) {
      result.setSettings(mapSettings(input.getSettings()));
    }
    return result;
  }

  private CorpUser getAuthor(String actor) {
    CorpUser partialUser = new CorpUser();
    partialUser.setUrn(actor);
    return partialUser;
  }

  private InstitutionalMemoryMetadataSettings mapSettings(
      com.linkedin.common.InstitutionalMemoryMetadataSettings gmsSettings) {
    InstitutionalMemoryMetadataSettings settings = new InstitutionalMemoryMetadataSettings();
    settings.setShowInAssetPreview(gmsSettings.isShowInAssetPreview());
    return settings;
  }
}

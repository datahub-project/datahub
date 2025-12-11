/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.corpgroup.mappers;

import com.linkedin.data.template.GetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CorpGroupProperties;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class CorpGroupPropertiesMapper
    implements ModelMapper<com.linkedin.identity.CorpGroupInfo, CorpGroupProperties> {

  public static final CorpGroupPropertiesMapper INSTANCE = new CorpGroupPropertiesMapper();

  public static CorpGroupProperties map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.identity.CorpGroupInfo corpGroupInfo) {
    return INSTANCE.apply(context, corpGroupInfo);
  }

  @Override
  public CorpGroupProperties apply(
      @Nullable QueryContext context, @Nonnull final com.linkedin.identity.CorpGroupInfo info) {
    final CorpGroupProperties result = new CorpGroupProperties();
    result.setEmail(info.getEmail());
    result.setDescription(info.getDescription());
    result.setDisplayName(info.getDisplayName());
    result.setSlack(info.getSlack(GetMode.DEFAULT));
    return result;
  }
}

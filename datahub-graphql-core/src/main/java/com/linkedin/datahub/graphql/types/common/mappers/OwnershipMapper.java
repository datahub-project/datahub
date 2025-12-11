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
import com.linkedin.datahub.graphql.generated.Ownership;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class OwnershipMapper {

  public static final OwnershipMapper INSTANCE = new OwnershipMapper();

  public static Ownership map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.Ownership ownership,
      @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(context, ownership, entityUrn);
  }

  public Ownership apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.Ownership ownership,
      @Nonnull final Urn entityUrn) {
    final Ownership result = new Ownership();
    result.setLastModified(AuditStampMapper.map(context, ownership.getLastModified()));
    result.setOwners(
        ownership.getOwners().stream()
            .map(owner -> OwnerMapper.map(context, owner, entityUrn))
            .collect(Collectors.toList()));
    return result;
  }
}

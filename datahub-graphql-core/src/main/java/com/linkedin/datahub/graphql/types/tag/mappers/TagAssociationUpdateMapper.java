/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.tag.mappers;

import com.linkedin.common.TagAssociation;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.TagAssociationUpdate;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class TagAssociationUpdateMapper
    implements ModelMapper<TagAssociationUpdate, TagAssociation> {

  public static final TagAssociationUpdateMapper INSTANCE = new TagAssociationUpdateMapper();

  public static TagAssociation map(
      @Nullable final QueryContext context,
      @Nonnull final TagAssociationUpdate tagAssociationUpdate) {
    return INSTANCE.apply(context, tagAssociationUpdate);
  }

  public TagAssociation apply(
      @Nullable final QueryContext context, final TagAssociationUpdate tagAssociationUpdate) {
    final TagAssociation output = new TagAssociation();
    try {
      output.setTag(TagUrn.createFromString(tagAssociationUpdate.getTag().getUrn()));
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          String.format(
              "Failed to update tag with urn %s, invalid urn",
              tagAssociationUpdate.getTag().getUrn()));
    }
    return output;
  }
}

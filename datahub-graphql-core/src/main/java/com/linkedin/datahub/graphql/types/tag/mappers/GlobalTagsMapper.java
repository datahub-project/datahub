package com.linkedin.datahub.graphql.types.tag.mappers;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Tag;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class GlobalTagsMapper {
  public static final GlobalTagsMapper INSTANCE = new GlobalTagsMapper();

  public static com.linkedin.datahub.graphql.generated.GlobalTags map(
      @Nonnull final GlobalTags standardTags, @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(standardTags, entityUrn);
  }

  public com.linkedin.datahub.graphql.generated.GlobalTags apply(
      @Nonnull final GlobalTags input, @Nonnull final Urn entityUrn) {
    final com.linkedin.datahub.graphql.generated.GlobalTags result =
        new com.linkedin.datahub.graphql.generated.GlobalTags();
    result.setTags(
        input.getTags().stream()
            .map(tag -> this.mapTagAssociation(tag, entityUrn))
            .collect(Collectors.toList()));
    return result;
  }

  private com.linkedin.datahub.graphql.generated.TagAssociation mapTagAssociation(
      @Nonnull final TagAssociation input, @Nonnull final Urn entityUrn) {
    final com.linkedin.datahub.graphql.generated.TagAssociation result =
        new com.linkedin.datahub.graphql.generated.TagAssociation();
    final Tag resultTag = new Tag();
    resultTag.setUrn(input.getTag().toString());
    result.setTag(resultTag);
    result.setAssociatedUrn(entityUrn.toString());
    return result;
  }
}

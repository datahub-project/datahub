package com.linkedin.datahub.graphql.types.tag.mappers;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Tag;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class GlobalTagsMapper {
  public static final GlobalTagsMapper INSTANCE = new GlobalTagsMapper();

  public static com.linkedin.datahub.graphql.generated.GlobalTags map(
      @Nullable final QueryContext context,
      @Nonnull final GlobalTags standardTags,
      @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(context, standardTags, entityUrn);
  }

  public com.linkedin.datahub.graphql.generated.GlobalTags apply(
      @Nullable final QueryContext context,
      @Nonnull final GlobalTags input,
      @Nonnull final Urn entityUrn) {
    final com.linkedin.datahub.graphql.generated.GlobalTags result =
        new com.linkedin.datahub.graphql.generated.GlobalTags();
    result.setTags(
        input.getTags().stream()
            .map(tag -> mapTagAssociation(context, tag, entityUrn))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList()));
    return result;
  }

  private static Optional<com.linkedin.datahub.graphql.generated.TagAssociation> mapTagAssociation(
      @Nullable final QueryContext context,
      @Nonnull final TagAssociation input,
      @Nonnull final Urn entityUrn) {

    final com.linkedin.datahub.graphql.generated.TagAssociation result =
        new com.linkedin.datahub.graphql.generated.TagAssociation();
    final Tag resultTag = new Tag();
    resultTag.setUrn(input.getTag().toString());
    result.setTag(resultTag);
    result.setAssociatedUrn(entityUrn.toString());
    return Optional.of(result);
  }
}

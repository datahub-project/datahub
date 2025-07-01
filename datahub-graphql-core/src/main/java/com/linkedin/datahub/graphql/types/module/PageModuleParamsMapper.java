package com.linkedin.datahub.graphql.types.module;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.LinkModuleParams;
import com.linkedin.datahub.graphql.generated.Post;
import com.linkedin.datahub.graphql.generated.RichTextModuleParams;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.module.DataHubPageModuleParams;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class PageModuleParamsMapper
    implements ModelMapper<
        DataHubPageModuleParams, com.linkedin.datahub.graphql.generated.DataHubPageModuleParams> {

  public static final PageModuleParamsMapper INSTANCE = new PageModuleParamsMapper();

  public static com.linkedin.datahub.graphql.generated.DataHubPageModuleParams map(
      @Nonnull final DataHubPageModuleParams params) {
    return INSTANCE.apply(null, params);
  }

  @Override
  public com.linkedin.datahub.graphql.generated.DataHubPageModuleParams apply(
      @Nullable final QueryContext context, @Nonnull final DataHubPageModuleParams params) {
    final com.linkedin.datahub.graphql.generated.DataHubPageModuleParams result =
        new com.linkedin.datahub.graphql.generated.DataHubPageModuleParams();

    // Map link params if present
    if (params.hasLinkParams()
        && params.getLinkParams() != null
        && params.getLinkParams().hasLinkUrn()
        && params.getLinkParams().getLinkUrn() != null) {
      LinkModuleParams linkModuleParams = new LinkModuleParams();
      Post link = new Post();
      link.setUrn(params.getLinkParams().getLinkUrn().toString());
      link.setType(EntityType.POST);
      linkModuleParams.setLink(link);
      result.setLinkParams(linkModuleParams);
    }

    // Map rich text params if present
    if (params.hasRichTextParams()) {
      RichTextModuleParams richTextParams = new RichTextModuleParams();
      richTextParams.setContent(params.getRichTextParams().getContent());
      result.setRichTextParams(richTextParams);
    }

    return result;
  }
}

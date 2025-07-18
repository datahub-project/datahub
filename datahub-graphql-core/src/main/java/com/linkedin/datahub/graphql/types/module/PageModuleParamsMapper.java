package com.linkedin.datahub.graphql.types.module;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AssetCollectionModuleParams;
import com.linkedin.datahub.graphql.generated.LinkModuleParams;
import com.linkedin.datahub.graphql.generated.RichTextModuleParams;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.module.DataHubPageModuleParams;
import java.util.List;
import java.util.stream.Collectors;
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
    if (params.getLinkParams() != null) {
      com.linkedin.module.LinkModuleParams linkParams = params.getLinkParams();

      if (linkParams.getLinkUrl() != null) {
        LinkModuleParams linkModuleParams = new LinkModuleParams();

        linkModuleParams.setLinkUrl(linkParams.getLinkUrl());

        if (linkParams.getImageUrl() != null) {
          linkModuleParams.setImageUrl(linkParams.getImageUrl());
        }
        if (linkParams.getDescription() != null) {
          linkModuleParams.setDescription(linkParams.getDescription());
        }
        result.setLinkParams(linkModuleParams);
      }
    }

    // Map rich text params if present
    if (params.hasRichTextParams()) {
      RichTextModuleParams richTextParams = new RichTextModuleParams();
      richTextParams.setContent(params.getRichTextParams().getContent());
      result.setRichTextParams(richTextParams);
    }

    // Map asset collection params if present
    if (params.getAssetCollectionParams() != null
        && params.getAssetCollectionParams().getAssetUrns() != null) {

      AssetCollectionModuleParams assetCollectionParams = new AssetCollectionModuleParams();

      List<String> assetUrnStrings =
          params.getAssetCollectionParams().getAssetUrns().stream()
              .map(Urn::toString)
              .collect(Collectors.toList());

      assetCollectionParams.setAssetUrns(assetUrnStrings);
      result.setAssetCollectionParams(assetCollectionParams);
    }

    return result;
  }
}

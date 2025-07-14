package com.linkedin.datahub.graphql.types.module;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AssetCollectionModuleParams;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.LinkModuleParams;
import com.linkedin.datahub.graphql.generated.Post;
import com.linkedin.datahub.graphql.generated.RichTextModuleParams;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
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
    if (params.getLinkParams() != null && params.getLinkParams().getLinkUrn() != null) {
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

    // Map asset collection params if present
    if (params.getAssetCollectionParams() != null
        && params.getAssetCollectionParams().getAssetUrns() != null) {

      AssetCollectionModuleParams assetCollectionParams = new AssetCollectionModuleParams();

      UrnArray urnArray = params.getAssetCollectionParams().getAssetUrns();
      List<Urn> urns = urnArray.stream().collect(Collectors.toList());

      List<Entity> assets =
          urns.stream()
              .map(urn -> UrnToEntityMapper.map(context, urn))
              .collect(Collectors.toList());

      assetCollectionParams.setAssets(assets);
      result.setAssetCollectionParams(assetCollectionParams);
    }

    return result;
  }
}

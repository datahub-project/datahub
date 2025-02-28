package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.SourceDetails;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Origin;
import com.linkedin.datahub.graphql.generated.OriginType;
import com.linkedin.datahub.graphql.generated.SyncMechanism;
import com.linkedin.datahub.graphql.types.mappers.MapperUtils;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class OriginMapper implements ModelMapper<com.linkedin.common.Origin, Origin> {

  public static final OriginMapper INSTANCE = new OriginMapper();

  public static Origin map(
      @Nullable final QueryContext context, @Nonnull final com.linkedin.common.Origin metadata) {
    return INSTANCE.apply(context, metadata);
  }

  @Override
  public Origin apply(
      @Nullable final QueryContext context, @Nonnull final com.linkedin.common.Origin input) {
    final Origin result = new Origin();
    if (input.hasType()) {
      result.setType(OriginType.valueOf(input.getType().toString()));
    } else {
      result.setType(OriginType.UNKNOWN);
    }
    if (input.getExternalType() != null) {
      result.setExternalType(input.getExternalType());
    }
    if (input.getSourceDetails() != null && input.getSourceDetails().size() > 0) {
      List<SourceDetails> sortedSourceDetails =
          input.getSourceDetails().stream()
              .sorted(Comparator.comparing(details -> details.getLastModified().getTime()))
              .collect(Collectors.toList());
      result.setResolvedSourceDetails(mapSourceDetails(context, sortedSourceDetails.get(0)));
      result.setRawSourceDetails(
          sortedSourceDetails.stream()
              .map(d -> mapSourceDetails(context, d))
              .collect(Collectors.toList()));
    }
    return result;
  }

  private com.linkedin.datahub.graphql.generated.SourceDetails mapSourceDetails(
      @Nullable final QueryContext context, @Nonnull final SourceDetails sourceDetails) {
    final com.linkedin.datahub.graphql.generated.SourceDetails result =
        new com.linkedin.datahub.graphql.generated.SourceDetails();
    result.setSource(UrnToEntityMapper.map(context, sourceDetails.getSource()));
    result.setPlatform(UrnToEntityMapper.map(context, sourceDetails.getPlatform()));
    result.setLastModified(MapperUtils.mapResolvedAuditStamp(sourceDetails.getLastModified()));
    result.setMechanism(SyncMechanism.valueOf(sourceDetails.getMechanism().toString()));
    if (sourceDetails.getProperties() != null) {
      result.setProperties(StringMapMapper.map(context, sourceDetails.getProperties()));
    }
    return result;
  }
}

package io.datahubproject.openlineage.converter;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.DataTemplate;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.event.EventFormatter;
import datahub.event.MetadataChangeProposalWrapper;
import java.io.IOException;

public final class OpenLineageMcpFactory {
  private OpenLineageMcpFactory() {}

  public static MetadataChangeProposal upsert(
      Urn entityUrn, String entityType, DataTemplate aspect) {
    return convertUnchecked(
        MetadataChangeProposalWrapper.create(
            builder ->
                builder.entityType(entityType).entityUrn(entityUrn).upsert().aspect(aspect)));
  }

  public static MetadataChangeProposal convert(MetadataChangeProposalWrapper wrapper)
      throws IOException {
    return new EventFormatter().convert(wrapper);
  }

  public static MetadataChangeProposal convertUnchecked(MetadataChangeProposalWrapper wrapper) {
    try {
      return convert(wrapper);
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
  }
}

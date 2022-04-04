package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.Operation;
import com.linkedin.datahub.graphql.generated.OperationType;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.types.mappers.TimeSeriesAspectMapper;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.utils.GenericRecordUtils;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class OperationMapper implements TimeSeriesAspectMapper<com.linkedin.datahub.graphql.generated.Operation> {

    public static final OperationMapper INSTANCE = new OperationMapper();

    public static com.linkedin.datahub.graphql.generated.Operation map(@Nonnull final EnvelopedAspect envelopedAspect) {
        return INSTANCE.apply(envelopedAspect);
    }

    @Override
    public com.linkedin.datahub.graphql.generated.Operation apply(@Nonnull final EnvelopedAspect envelopedAspect) {

        Operation gmsProfile = GenericRecordUtils
                .deserializeAspect(
                        envelopedAspect.getAspect().getValue(),
                        envelopedAspect.getAspect().getContentType(),
                        Operation.class);

        final com.linkedin.datahub.graphql.generated.Operation result =
                new com.linkedin.datahub.graphql.generated.Operation();

        result.setTimestampMillis(gmsProfile.getTimestampMillis());
        result.setLastUpdatedTimestamp(gmsProfile.getLastUpdatedTimestamp());
        if (gmsProfile.hasActor()) {
            result.setActor(gmsProfile.getActor().toString());
        }
        result.setOperationType(OperationType.valueOf(OperationType.class, gmsProfile.getOperationType().toString()));
        if (gmsProfile.hasNumAffectedRows()) {
            result.setNumAffectedRows(gmsProfile.getNumAffectedRows());
        }
        if (gmsProfile.hasAffectedDatasets()) {
            result.setAffectedDatasets(gmsProfile.getAffectedDatasets().stream().map(Urn::toString).collect(Collectors.toList()));
        }

        return result;
    }
}

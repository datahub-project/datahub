package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.datahub.graphql.generated.TimeSeriesAspect;
import com.linkedin.metadata.aspect.EnvelopedAspect;

public interface TimeSeriesAspectMapper<T extends TimeSeriesAspect>
    extends ModelMapper<EnvelopedAspect, T> {}

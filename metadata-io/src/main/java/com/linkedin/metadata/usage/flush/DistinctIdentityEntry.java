package com.linkedin.metadata.usage.flush;

import io.datahubproject.metadata.context.usage.AttributionType;

public record DistinctIdentityEntry(String usageIdentity, AttributionType attributionType) {}

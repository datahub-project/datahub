package com.linkedin.metadata.ratelimit;

import com.linkedin.metadata.ratelimit.model.RateLimitSource;
import javax.annotation.Nullable;

record RateLimitContext(
    String path,
    String method,
    @Nullable String operationName,
    @Nullable String actorUrn,
    RateLimitSource source) {}

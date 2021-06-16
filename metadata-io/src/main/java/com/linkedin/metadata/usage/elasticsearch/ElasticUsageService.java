package com.linkedin.metadata.usage.elasticsearch;

import com.linkedin.metadata.usage.UsageService;

import javax.annotation.Nonnull;

public class ElasticUsageService implements UsageService {
    @Override
    public void configure() {
        // TODO configure this
    }

    @Override
    public void upsertDocument(@Nonnull String document, @Nonnull String docId) {
        // TODO upsert document
    }
}

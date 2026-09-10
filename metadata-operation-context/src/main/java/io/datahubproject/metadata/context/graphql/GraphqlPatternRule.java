package io.datahubproject.metadata.context.graphql;

import io.datahubproject.metadata.context.usage.UsageOperation;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

/** Precompiled GraphQL name pattern mapped to a usage operation bucket. */
public record GraphqlPatternRule(@Nonnull UsageOperation operation, @Nonnull Pattern pattern) {}

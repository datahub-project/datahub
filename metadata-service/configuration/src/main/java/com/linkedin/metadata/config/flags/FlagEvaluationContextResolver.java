package com.linkedin.metadata.config.flags;

import com.datahub.context.OperationFingerprint;
import dev.openfeature.sdk.EvaluationContext;
import javax.annotation.Nonnull;

/**
 * Derives the OpenFeature {@link EvaluationContext} — the targeting identity a {@link
 * dev.openfeature.sdk.FeatureProvider} evaluates against — from the in-flight operation.
 *
 * <p>This is the seam that keeps deployment-specific identity out of {@link FeatureFlagProvider}.
 * OSS targets nothing: one deployment, one set of values. An extension module contributes a
 * {@code @Primary} bean that targets on whatever identity it isolates by, without the facade or any
 * caller learning such an identity exists.
 *
 * <p>It is a separate interface rather than inline in the facade because a {@link
 * dev.openfeature.sdk.FeatureProvider} is handed an {@link EvaluationContext} and never the
 * operation — and an {@code EvaluationContext} can only carry primitive-ish values, so the
 * operation cannot be smuggled through it. The translation therefore has to happen before the
 * provider is called, in something a deployment can swap.
 *
 * <p>Takes an {@link OperationFingerprint} rather than {@code OperationContext} so this SPI can
 * live in {@code metadata-service:configuration} without that module depending on {@code
 * metadata-operation-context}. {@code OperationContext} implements {@link OperationFingerprint}, so
 * callers holding a full context pass it straight through. Mirrors {@code IndexPrefixResolver}.
 *
 * <p>An implementation that derives targeting from the operation must still return a usable context
 * when the operation carries no such identity ({@link OperationFingerprint#EMPTY}, bootstrap and
 * system paths). Flags are read on startup paths, where an exception is a far worse outcome than a
 * deployment-default value.
 */
@FunctionalInterface
public interface FlagEvaluationContextResolver {

  @Nonnull
  EvaluationContext resolve(@Nonnull OperationFingerprint operation);
}

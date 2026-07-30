package com.datahub.context;

/**
 * Marker interface for request-scoped or context-scoped enrichments carried on {@link
 * OperationFingerprint} / {@code OperationContext}.
 *
 * <p>Enrichments are typed value objects that cross-cutting concerns can stamp onto an in-flight
 * operation and downstream layers can read without knowing about the concern. Examples of
 * enrichment kinds a deployment might add: a tenant identifier, a per-tenant configuration record,
 * a feature-flag set, a billing/cost-attribution tag, a rate-limit budget.
 *
 * <p>Storage is keyed by concrete class — one enrichment per implementing class per operation.
 * Callers retrieve via {@link OperationFingerprint#getEnrichment(Class)} with the concrete class,
 * receiving an {@link java.util.Optional} of that type. No string keys, no casts.
 *
 * <p>Enrichment classes should be immutable value classes (Java records preferred). They may carry
 * helper methods; they must not hold service references or mutable state.
 *
 * <p>OSS ships this marker and the storage/retrieval plumbing. Concrete enrichment classes live in
 * whichever module owns the concept (deployment-specific modules for deployment concerns).
 */
public interface Enrichment {}

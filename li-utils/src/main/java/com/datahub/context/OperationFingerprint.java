package com.datahub.context;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Lightweight fingerprint interface representing the minimal identity surface of an in-flight
 * operation. Implemented by {@code OperationContext} so callers that hold a full context can pass
 * it through without casting.
 *
 * <p>Lives in li-utils to avoid circular dependencies between entity-registry and
 * metadata-operation-context.
 */
public interface OperationFingerprint {

  /**
   * No-op singleton for bootstrap / test contexts where no real operation context is available.
   * Returns stub values that are safe to use for read-routing in single-tenant deployments.
   */
  OperationFingerprint EMPTY =
      new OperationFingerprint() {
        private static final String EMPTY_ID = "empty";
        private static final Urn EMPTY_ACTOR = UrnUtils.getUrn("urn:li:corpuser:UNKNOWN");
        private static final AuditStamp EMPTY_AUDIT_STAMP =
            new AuditStamp().setActor(EMPTY_ACTOR).setTime(0L);

        @Nonnull
        @Override
        public Urn getActor() {
          return EMPTY_ACTOR;
        }

        @Nonnull
        @Override
        public String getRequestID() {
          return EMPTY_ID;
        }

        @Nonnull
        @Override
        public AuditStamp getAuditStamp() {
          return EMPTY_AUDIT_STAMP;
        }

        @Nonnull
        @Override
        public String getGlobalContextId() {
          return EMPTY_ID;
        }

        @Nonnull
        @Override
        public String getSearchContextId() {
          return EMPTY_ID;
        }

        @Nonnull
        @Override
        public String getEntityContextId() {
          return EMPTY_ID;
        }

        @Nonnull
        @Override
        public <T extends Enrichment> Optional<T> getEnrichment(@Nonnull final Class<T> type) {
          return Optional.empty();
        }
      };

  @Nonnull
  Urn getActor();

  @Nonnull
  String getRequestID();

  @Nonnull
  AuditStamp getAuditStamp();

  @Nonnull
  String getGlobalContextId();

  @Nonnull
  String getSearchContextId();

  @Nonnull
  String getEntityContextId();

  /**
   * Return the {@link Enrichment} of the given concrete class stamped onto this operation, or
   * {@link Optional#empty()} if no such enrichment is present.
   *
   * <p>Abstract — every implementation must decide explicitly whether it carries enrichments.
   * {@code OperationContext} propagates enrichments from its request-side {@link
   * io.datahubproject.metadata.context.RequestContext}; other implementations (e.g. the {@link
   * #EMPTY} bootstrap singleton) return {@link Optional#empty()}. See {@link Enrichment} for the
   * extension pattern.
   */
  @Nonnull
  <T extends Enrichment> Optional<T> getEnrichment(@Nonnull final Class<T> type);
}

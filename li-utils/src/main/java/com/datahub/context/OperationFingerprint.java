package com.datahub.context;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
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
        private final AuditStamp EMPTY_AUDIT_STAMP =
            new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:UNKNOWN")).setTime(0L);

        @Nonnull
        @Override
        public Urn getActor() {
          return UrnUtils.getUrn("urn:li:corpuser:UNKNOWN");
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
}

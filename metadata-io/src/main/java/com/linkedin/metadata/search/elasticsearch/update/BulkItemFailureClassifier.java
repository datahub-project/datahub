package com.linkedin.metadata.search.elasticsearch.update;

import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.core.rest.RestStatus;

/** Classifies bulk item / transport failures for requeue vs LWW vs unrecovered transfer. */
public final class BulkItemFailureClassifier {
  private BulkItemFailureClassifier() {}

  /**
   * A create-only write found the document already there. Unlike an update conflict, a retry
   * conflicts again, so it must not be requeued.
   */
  public static boolean isCreateConflict(
      @Nullable DocWriteRequest<?> request,
      @Nullable RestStatus status,
      @Nullable String failureTypeOrMessage) {
    return request != null
        && request.opType() == DocWriteRequest.OpType.CREATE
        && (status == RestStatus.CONFLICT || isVersionConflict(failureTypeOrMessage));
  }

  public static boolean isVersionConflict(@Nullable String failureTypeOrMessage) {
    if (failureTypeOrMessage == null) {
      return false;
    }
    String lower = StringUtils.toRootLowerCase(failureTypeOrMessage);
    return lower.contains("version_conflict_engine_exception")
        || lower.contains("version_conflict");
  }

  public static boolean isDocumentMissing(@Nullable String failureTypeOrMessage) {
    if (failureTypeOrMessage == null) {
      return false;
    }
    return StringUtils.toRootLowerCase(failureTypeOrMessage).contains("document_missing_exception");
  }

  public static boolean isRetriableStatus(@Nullable RestStatus status) {
    if (status == null) {
      return false;
    }
    return status == RestStatus.TOO_MANY_REQUESTS
        || status == RestStatus.SERVICE_UNAVAILABLE
        || status == RestStatus.GATEWAY_TIMEOUT
        || status == RestStatus.CONFLICT;
  }

  public static boolean isRetriableFailure(
      @Nullable RestStatus status, @Nullable String failureTypeOrMessage) {
    if (isDocumentMissing(failureTypeOrMessage)) {
      return false;
    }
    if (isVersionConflict(failureTypeOrMessage) || status == RestStatus.CONFLICT) {
      return true;
    }
    return isRetriableStatus(status)
        || (failureTypeOrMessage != null
            && StringUtils.toRootLowerCase(failureTypeOrMessage).contains("rejected_execution"));
  }
}

package com.linkedin.metadata.entity.upgrade;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.restli.client.RestLiResponseException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;

/**
 * Store backed by {@link SystemEntityClient}, so the caller needs no datasource. The client
 * resolves in-process in GMS and over REST in the standalone MAE consumer.
 */
@RequiredArgsConstructor
public class EntityClientUpgradeResultStore implements DataHubUpgradeResultStore {

  /** Longest cause chain walked when looking for the precondition marker. */
  private static final int MAX_CAUSE_DEPTH = 20;

  @Nonnull private final SystemEntityClient entityClient;

  @Override
  @Nullable
  public EnvelopedAspect readLatest(OperationContext opContext, @Nonnull final Urn upgradeIdUrn)
      throws Exception {
    // NoCache: a cached read would hand back a stale systemMetadata.version and make every
    // conditional write conflict, and would also hide swap-state transitions from the poller.
    final Map<Urn, EntityResponse> responses =
        entityClient.batchGetV2NoCache(
            opContext,
            Constants.DATA_HUB_UPGRADE_ENTITY_NAME,
            Set.of(upgradeIdUrn),
            Set.of(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME));

    final EntityResponse response = responses == null ? null : responses.get(upgradeIdUrn);
    if (response == null) {
      return null;
    }
    // GetMode.NULL: aspects is a required field, so the default STRICT getter throws rather than
    // returning null. An absent aspect has to read as absent here — mergeAndPersist calls this
    // inside its compare-and-set loop with no catch, and would abort the write instead.
    final EnvelopedAspectMap aspects = response.getAspects(GetMode.NULL);
    return aspects == null ? null : aspects.get(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME);
  }

  @Override
  public void ingest(OperationContext opContext, @Nonnull final MetadataChangeProposal proposal)
      throws Exception {
    try {
      entityClient.ingestProposal(opContext, proposal, false);
    } catch (ValidationException e) {
      // In-process (SystemJavaEntityClient): EntityService's exception reaches us unwrapped,
      // collection and all. Pass it through rather than rebuilding a lossy copy.
      throw e;
    } catch (Exception e) {
      // Over the wire (SystemRestliEntityClient): the resource maps a failed precondition to 422
      // and only the message survives (AspectResource#ingestProposals). Re-raise it as the
      // ValidationException the retry loop understands, so both transports fail the same way.
      final String conflict = versionConflictMessage(e);
      if (conflict != null) {
        throw new ValidationException(conflict);
      }
      throw e;
    }
  }

  /**
   * The server's rejection of our {@code If-Version-Match} precondition, or {@code null} if this
   * failure is something else. Matches on the validator's message because restli erases the
   * exception type at the wire boundary.
   *
   * <p>Deliberately one method rather than a detect/extract pair: those can disagree — detecting on
   * a nested cause while extracting the outer message yields a {@code ValidationException} the
   * retry loop no longer recognises, so a conflict aborts instead of retrying.
   */
  @Nullable
  private static String versionConflictMessage(@Nullable final Throwable throwable) {
    Throwable t = throwable;
    // Depth-capped rather than following the chain to its end: a self-referential or cyclic cause
    // would otherwise spin forever.
    for (int depth = 0; t != null && depth < MAX_CAUSE_DEPTH; depth++, t = t.getCause()) {
      if (t instanceof RestLiResponseException) {
        final RestLiResponseException restLi = (RestLiResponseException) t;
        if (restLi.hasServiceErrorMessage()
            && DataHubUpgradeResultConditionalPersist.isVersionMismatchMessage(
                restLi.getServiceErrorMessage())) {
          return restLi.getServiceErrorMessage();
        }
      }
      if (DataHubUpgradeResultConditionalPersist.isVersionMismatchMessage(t.getMessage())) {
        return t.getMessage();
      }
      if (t.getCause() == t) {
        break;
      }
    }
    return null;
  }
}

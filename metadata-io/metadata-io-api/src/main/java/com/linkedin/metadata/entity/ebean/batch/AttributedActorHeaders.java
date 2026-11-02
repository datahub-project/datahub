package com.linkedin.metadata.entity.ebean.batch;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * MCP header carrying the caller of a queued (async) write.
 *
 * <p>GMS checks the caller's credentials, then queues the MCP; the MCE consumer later writes it
 * back with system credentials. Without this header the write is attributed to the system. Stored
 * on {@link MetadataChangeProposal#getHeaders()} (like {@code UsageDedupHeaders}) so it survives
 * the queue and per-item attribution works in batched consumers.
 *
 * <p>Only honored for system-authenticated writes: anything that can write the MCP topic directly
 * is trusted to set it, while REST callers cannot.
 */
@Slf4j
public final class AttributedActorHeaders {

  public static final String ATTRIBUTED_ACTOR = Constants.ATTRIBUTED_ACTOR_HEADER_NAME;

  private AttributedActorHeaders() {}

  public static void stamp(@Nonnull MetadataChangeProposal mcp, @Nonnull String actorUrn) {
    if (!mcp.hasHeaders()) {
      mcp.setHeaders(new StringMap());
    }
    strip(mcp);
    mcp.getHeaders().put(ATTRIBUTED_ACTOR, actorUrn);
  }

  private static void strip(@Nonnull MetadataChangeProposal mcp) {
    if (!mcp.hasHeaders()) {
      return;
    }
    StringMap headers = mcp.getHeaders();
    headers.keySet().stream()
        .filter(ATTRIBUTED_ACTOR::equalsIgnoreCase)
        .collect(Collectors.toList())
        .forEach(headers::remove);
  }

  /**
   * The stamp for a write of {@code mcp}: the attributed caller, with the writer as impersonator,
   * when {@code trusted} and the header names a different actor; otherwise {@code callerStamp}.
   */
  @Nonnull
  public static AuditStamp resolve(
      @Nonnull MetadataChangeProposal mcp, @Nonnull AuditStamp callerStamp, boolean trusted) {
    if (!trusted) {
      return callerStamp;
    }
    String value = headerValue(mcp);
    if (value == null || value.isBlank()) {
      return callerStamp;
    }
    final Urn actor;
    try {
      actor = UrnUtils.getUrn(value);
    } catch (RuntimeException e) {
      log.warn("Ignoring malformed {} header value: {}", ATTRIBUTED_ACTOR, value);
      return callerStamp;
    }
    if (actor.equals(callerStamp.getActor())) {
      return callerStamp;
    }
    return new AuditStamp()
        .setActor(actor)
        .setImpersonator(callerStamp.getActor())
        .setTime(callerStamp.getTime());
  }

  @Nullable
  private static String headerValue(@Nonnull MetadataChangeProposal mcp) {
    if (!mcp.hasHeaders()) {
      return null;
    }
    return mcp.getHeaders().entrySet().stream()
        .filter(entry -> ATTRIBUTED_ACTOR.equalsIgnoreCase(entry.getKey()))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElse(null);
  }
}

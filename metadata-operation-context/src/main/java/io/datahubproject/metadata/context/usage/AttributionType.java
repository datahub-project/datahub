package io.datahubproject.metadata.context.usage;

import io.datahubproject.metadata.context.AgentClass;

/**
 * Coarse initiation class for distinct usage-identity rows (MAU-style metrics).
 *
 * <p>Complements {@link AgentClass} (User-Agent-derived client type). Where {@code AgentClass}
 * describes <em>what client</em> sent the request, {@code AttributionType} describes <em>who
 * initiated</em> the activity for human-vs-automated analytics — similar to {@code
 * interaction_source} fields used in product analytics (human vs agent) and bot-detection products
 * that classify sessions as {@code human}, {@code ai_agent}, or {@code bot}.
 *
 * <p>IETF Web Bot Auth and related drafts distinguish human clients from automated agents and
 * crawlers for rate limiting and measurement; this enum provides a stable, low-cardinality rollup
 * aligned with that split.
 *
 * @see AgentClass
 */
public enum AttributionType {
  /** Direct human interaction (browser, mobile app, CLI). */
  HUMAN("human"),

  /**
   * AI agent or assistant acting on behalf of a human (set explicitly at call sites when known).
   */
  AGENT_MEDIATED("agent_mediated"),

  /** Programmatic non-human clients: SDK, ingestion pipelines, HTTP libraries. */
  AUTOMATED("automated"),

  /** Crawlers, robots, and indexers. */
  BOT("bot"),

  /** Insufficient signals to classify (maps from {@link AgentClass#UNKNOWN} and similar). */
  UNKNOWN("unknown");

  private final String dimensionValue;

  AttributionType(String dimensionValue) {
    this.dimensionValue = dimensionValue;
  }

  public String dimensionValue() {
    return dimensionValue;
  }

  /**
   * Default attribution from {@link AgentClass}. explicitly. {@link #AGENT_MEDIATED} is never
   * inferred here — it must be set when an AI agent path is known (e.g. GraphQL MCP, Ask DataHub).
   */
  public static AttributionType fromAgentClass(AgentClass agentClass) {
    return switch (agentClass) {
      case BROWSER, MOBILE_APP, EMAIL_CLIENT, CLI -> HUMAN;
      case INGESTION, SDK, LIBRARY, SYSTEM -> AUTOMATED;
      case ROBOT, CRAWLER -> BOT;
      case HACKER, UNKNOWN -> UNKNOWN;
    };
  }
}

package datahub.client.v2.entity;

/**
 * Indicates the source of a cached aspect.
 *
 * <p>This enum is used by {@link AspectCache} to track whether an aspect was fetched from the
 * server or created/modified locally.
 */
public enum AspectSource {
  /**
   * Aspect was fetched from the DataHub server.
   *
   * <p>SERVER-sourced aspects are subject to TTL expiration.
   */
  SERVER,

  /**
   * Aspect was created or modified locally.
   *
   * <p>LOCAL-sourced aspects do not expire and always represent pending writes.
   */
  LOCAL
}

package com.linkedin.datahub.graphql.resolvers.settings.mcp;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.DATAHUB_VIEW_ENTITY_NAME;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.McpServerConfigInput;
import com.linkedin.datahub.graphql.generated.UpdateMcpSettingsInput;
import com.linkedin.datahub.graphql.types.agent.AgentToolRefMapper;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.McpInstructionSize;
import com.linkedin.settings.global.McpServerConfig;
import com.linkedin.settings.global.McpServerConfigArray;
import com.linkedin.settings.global.McpSettings;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Updates the global MCP (Model Context Protocol) server settings.
 *
 * <p>Scoped servers are upserted by slug; servers not in the input are left unchanged. Creating a
 * scoped server whose slug is not already stored requires the createMcpServersEnabled feature flag.
 * Requires the 'Manage Platform Settings' privilege.
 */
public class UpdateMcpSettingsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  /**
   * Slugs reserved by the platform and not allowed for scoped MCP servers. "default",
   * "datahub-default", and "datahub" denote the default server (exposed at /mcp with no slug);
   * "mcp" would collide with the mount path itself.
   */
  private static final Set<String> RESERVED_MCP_SLUGS =
      Set.of("default", "datahub-default", "datahub", "mcp");

  private static final Pattern SLUG_PATTERN = Pattern.compile("[a-z0-9][a-z0-9-]*");

  private final SettingsService _settingsService;
  private final FeatureFlags _featureFlags;

  public UpdateMcpSettingsResolver(
      @Nonnull final SettingsService settingsService, @Nonnull final FeatureFlags featureFlags) {
    _settingsService = Objects.requireNonNull(settingsService, "settingsService must not be null");
    _featureFlags = Objects.requireNonNull(featureFlags, "featureFlags must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateMcpSettingsInput input =
        bindArgument(environment.getArgument("input"), UpdateMcpSettingsInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canManageGlobalSettings(context)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          try {
            // Read-modify-write of the single GlobalSettingsInfo aspect.
            final GlobalSettingsInfo maybeGlobalSettings =
                _settingsService.getGlobalSettings(context.getOperationContext());
            final GlobalSettingsInfo newGlobalSettings =
                maybeGlobalSettings != null ? maybeGlobalSettings : new GlobalSettingsInfo();
            final McpSettings mcpSettings =
                newGlobalSettings.hasMcpSettings()
                    ? newGlobalSettings.getMcpSettings()
                    : new McpSettings();

            updateMcpSettings(
                mcpSettings,
                input,
                UrnUtils.getUrn(context.getActorUrn()),
                System.currentTimeMillis());
            newGlobalSettings.setMcpSettings(mcpSettings);

            _settingsService.updateGlobalSettings(context.getOperationContext(), newGlobalSettings);
            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to update MCP settings! %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void updateMcpSettings(
      @Nonnull final McpSettings settings,
      @Nonnull final UpdateMcpSettingsInput update,
      @Nonnull final Urn actor,
      final long now) {
    if (update.getEnabled() != null) {
      settings.setEnabled(update.getEnabled());
    }
    if (update.getObservationTelemetryEnabled() != null) {
      settings.setObservationTelemetryEnabled(update.getObservationTelemetryEnabled());
    }
    if (update.getObservationToolEnabled() != null) {
      settings.setObservationToolEnabled(update.getObservationToolEnabled());
    }

    if (update.getServers() != null) {
      // Insertion-ordered so existing servers keep their position and new ones are appended.
      final Map<String, McpServerConfig> serversBySlug = new LinkedHashMap<>();
      if (settings.hasServers()) {
        for (McpServerConfig existing : settings.getServers()) {
          if (existing.hasSlug()) {
            serversBySlug.put(existing.getSlug(), existing);
          }
        }
      }
      for (McpServerConfigInput serverInput : update.getServers()) {
        final String slug = validateSlug(serverInput.getSlug());
        final McpServerConfig existing = serversBySlug.get(slug);
        if (existing == null && !_featureFlags.isCreateMcpServersEnabled()) {
          throw new IllegalArgumentException(
              "Creating additional MCP servers is disabled; only the default server can be"
                  + " customized (slug: '"
                  + slug
                  + "')");
        }
        serversBySlug.put(slug, toServerConfig(serverInput, existing, actor, now).setSlug(slug));
      }
      settings.setServers(new McpServerConfigArray(serversBySlug.values()));
    }

    if (update.getDefaultMcpServerOverrides() != null) {
      final McpServerConfigInput overrideInput = update.getDefaultMcpServerOverrides();
      final McpServerConfig existingOverride =
          settings.hasDefaultMcpServerOverrides() ? settings.getDefaultMcpServerOverrides() : null;
      if (isEffectivelyEmptyOverride(overrideInput, existingOverride)) {
        // An override that changes nothing about serving is the same as no override. Clearing it
        // lets admins revert the default server to its built-in behaviour by blanking it out.
        settings.setDefaultMcpServerOverrides(null, SetMode.REMOVE_IF_NULL);
      } else {
        settings.setDefaultMcpServerOverrides(
            toServerConfig(overrideInput, existingOverride, actor, now));
      }
    }
  }

  @Nonnull
  private static String validateSlug(@Nullable final String slug) {
    if (slug == null || slug.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "MCP server config requires a non-empty slug (slug is required for scoped servers)");
    }
    if (!SLUG_PATTERN.matcher(slug).matches()) {
      throw new IllegalArgumentException(
          "MCP server slug must match [a-z0-9][a-z0-9-]* (got: '" + slug + "')");
    }
    if (RESERVED_MCP_SLUGS.contains(slug)) {
      throw new IllegalArgumentException(
          "MCP server slug '"
              + slug
              + "' is reserved; choose a different slug (reserved: "
              + RESERVED_MCP_SLUGS
              + ")");
    }
    return slug;
  }

  /**
   * Whether a default-override input is equivalent to "no override": not explicitly disabled, and
   * no custom instructions, tool restriction, scoping View, description, non-default instruction
   * size, or stored skills. {@code name} is ignored: it is cosmetic and unused at serve time.
   */
  private static boolean isEffectivelyEmptyOverride(
      @Nonnull final McpServerConfigInput input, @Nullable final McpServerConfig existing) {
    if (input.getEnabled() != null && !input.getEnabled()) {
      return false;
    }
    // Skills cannot be set through this API but are kept on update, so an override holding them
    // still matters.
    if (existing != null && existing.hasSkills() && !existing.getSkills().isEmpty()) {
      return false;
    }
    return isBlank(input.getCustomInstructions())
        && (input.getTools() == null || input.getTools().isEmpty())
        && isBlank(input.getViewUrn())
        && isBlank(input.getDescription())
        && (input.getInstructionSize() == null
            || input.getInstructionSize()
                == com.linkedin.datahub.graphql.generated.McpInstructionSize.LARGE);
  }

  private static boolean isBlank(@Nullable final String s) {
    return s == null || s.trim().isEmpty();
  }

  /** Builds the stored config from the input, replacing the existing one but keeping its skills. */
  @Nonnull
  private static McpServerConfig toServerConfig(
      @Nonnull final McpServerConfigInput input,
      @Nullable final McpServerConfig existing,
      @Nonnull final Urn actor,
      final long now) {
    // The slug is set by the caller: scoped servers have one, the default server's overrides do
    // not.
    final McpServerConfig config = new McpServerConfig();
    config.setName(input.getName());
    config.setEnabled(input.getEnabled() != null ? input.getEnabled() : true);
    if (input.getDescription() != null) {
      config.setDescription(input.getDescription());
    }
    if (input.getCustomInstructions() != null) {
      config.setCustomInstructions(input.getCustomInstructions());
    }
    if (input.getInstructionSize() != null) {
      config.setInstructionSize(McpInstructionSize.valueOf(input.getInstructionSize().name()));
    }
    if (input.getTools() != null) {
      config.setTools(AgentToolRefMapper.toGms(input.getTools()));
    }
    if (!isBlank(input.getViewUrn())) {
      config.setViewUrn(parseViewUrn(input.getViewUrn().trim()));
    }
    // Skills are not exposed by this API yet; keep whatever is stored.
    if (existing != null && existing.hasSkills()) {
      config.setSkills(existing.getSkills());
    }

    final AuditStamp stamp = new AuditStamp().setTime(now).setActor(actor);
    config.setCreated(existing != null && existing.hasCreated() ? existing.getCreated() : stamp);
    config.setLastModified(stamp);
    return config;
  }

  /**
   * Rejects malformed URNs and URNs that are not dataHubView URNs with a client error, rather than
   * a server error on parse or a later failed View load.
   */
  @Nonnull
  private static Urn parseViewUrn(@Nonnull final String viewUrn) {
    final Urn urn;
    try {
      urn = Urn.createFromString(viewUrn);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "MCP server viewUrn is not a valid URN: '" + viewUrn + "'");
    }
    if (!DATAHUB_VIEW_ENTITY_NAME.equals(urn.getEntityType())) {
      throw new IllegalArgumentException(
          "MCP server viewUrn must be a dataHubView URN (got entity type '"
              + urn.getEntityType()
              + "')");
    }
    return urn;
  }
}

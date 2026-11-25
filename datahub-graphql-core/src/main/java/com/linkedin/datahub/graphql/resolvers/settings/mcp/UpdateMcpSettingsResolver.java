package com.linkedin.datahub.graphql.resolvers.settings.mcp;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.agent.AIAgentToolRef;
import com.linkedin.agent.AIAgentToolRefArray;
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
import com.linkedin.metadata.aspect.validation.McpSettingsValidator;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.McpInstructionSize;
import com.linkedin.settings.global.McpServerConfig;
import com.linkedin.settings.global.McpServerConfigArray;
import com.linkedin.settings.global.McpSettings;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Updates the global MCP (Model Context Protocol) server settings.
 *
 * <p>Scoped servers are upserted by slug; servers not in the input are left unchanged. Creating a
 * scoped server whose slug is not already stored requires the createMcpServersEnabled feature flag.
 * Requires the 'Manage Platform Settings' privilege. The slug and View rules are enforced for every
 * write path by {@link McpSettingsValidator}; they are checked here too so a bad input fails with a
 * clear message before anything is written.
 */
public class UpdateMcpSettingsResolver implements DataFetcher<CompletableFuture<Boolean>> {

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
    final String error = McpSettingsValidator.slugError(slug);
    if (error != null) {
      throw new IllegalArgumentException(error);
    }
    return slug;
  }

  /**
   * Whether a default-override input is equivalent to "no override": not explicitly disabled, and
   * no name, custom instructions, tool restriction, scoping View, description, non-default
   * instruction size, or stored value this API keeps but cannot set.
   */
  private static boolean isEffectivelyEmptyOverride(
      @Nonnull final McpServerConfigInput input, @Nullable final McpServerConfig existing) {
    if (input.getEnabled() != null && !input.getEnabled()) {
      return false;
    }
    if (existing != null && holdsValuesThisApiCannotSet(existing)) {
      return false;
    }
    return isBlank(input.getName())
        && isBlank(input.getCustomInstructions())
        && (input.getTools() == null || input.getTools().isEmpty())
        && isBlank(input.getViewUrn())
        && isBlank(input.getDescription())
        && (input.getInstructionSize() == null
            || input.getInstructionSize()
                == com.linkedin.datahub.graphql.generated.McpInstructionSize.LARGE);
  }

  /**
   * Skills, a domain scope, and tool refs of a type the GraphQL enum lacks are kept on update (see
   * {@link #toServerConfig}), so an override holding them still matters.
   */
  private static boolean holdsValuesThisApiCannotSet(@Nonnull final McpServerConfig existing) {
    return (existing.hasSkills() && !existing.getSkills().isEmpty())
        || existing.hasDomainUrn()
        || !AgentToolRefMapper.unrepresentable(existing.getTools()).isEmpty();
  }

  private static boolean isBlank(@Nullable final String s) {
    return s == null || s.trim().isEmpty();
  }

  /**
   * Builds the stored config from the input. Every field the input carries is replaced (a missing
   * optional value clears it); fields it does not carry, such as skills and domainUrn, are kept
   * from the existing config.
   */
  @Nonnull
  private static McpServerConfig toServerConfig(
      @Nonnull final McpServerConfigInput input,
      @Nullable final McpServerConfig existing,
      @Nonnull final Urn actor,
      final long now) {
    // The slug is set by the caller: scoped servers have one, the default server's overrides do
    // not.
    final McpServerConfig config = existing != null ? copy(existing) : new McpServerConfig();
    config.setName(input.getName());
    config.setEnabled(input.getEnabled() != null ? input.getEnabled() : true);
    config.setDescription(input.getDescription(), SetMode.REMOVE_IF_NULL);
    config.setCustomInstructions(input.getCustomInstructions(), SetMode.REMOVE_IF_NULL);
    config.setInstructionSize(
        input.getInstructionSize() != null
            ? McpInstructionSize.valueOf(input.getInstructionSize().name())
            : null,
        SetMode.REMOVE_IF_NULL);
    config.setTools(
        mergeTools(input, existing != null ? existing.getTools() : null), SetMode.REMOVE_IF_NULL);
    config.setViewUrn(
        isBlank(input.getViewUrn()) ? null : parseViewUrn(input.getViewUrn().trim()),
        SetMode.REMOVE_IF_NULL);

    final AuditStamp stamp = new AuditStamp().setTime(now).setActor(actor);
    config.setCreated(existing != null && existing.hasCreated() ? existing.getCreated() : stamp);
    config.setLastModified(stamp);
    return config;
  }

  @Nonnull
  private static McpServerConfig copy(@Nonnull final McpServerConfig config) {
    try {
      return config.copy();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException("Failed to copy MCP server config", e);
    }
  }

  /**
   * The input's tool refs plus the stored refs whose type GraphQL cannot represent (e.g. PLUGIN, or
   * a type from a newer schema). Those are left out of reads, so a client saving what it loaded
   * cannot see them; keeping them stops a load-and-save from dropping or retyping them.
   */
  @Nullable
  private static AIAgentToolRefArray mergeTools(
      @Nonnull final McpServerConfigInput input, @Nullable final AIAgentToolRefArray stored) {
    final List<AIAgentToolRef> kept = AgentToolRefMapper.unrepresentable(stored);
    if (input.getTools() == null && kept.isEmpty()) {
      return null;
    }
    final AIAgentToolRefArray tools =
        input.getTools() != null
            ? AgentToolRefMapper.toGms(input.getTools())
            : new AIAgentToolRefArray();
    tools.addAll(kept);
    return tools;
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
    final String error = McpSettingsValidator.viewUrnError(urn);
    if (error != null) {
      throw new IllegalArgumentException(error);
    }
    return urn;
  }
}

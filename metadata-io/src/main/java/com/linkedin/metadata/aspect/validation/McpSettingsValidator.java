package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DATAHUB_VIEW_ENTITY_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.McpServerConfig;
import com.linkedin.settings.global.McpSettings;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Validates the MCP server settings on {@code globalSettingsInfo} across all write APIs: every
 * scoped server needs a well-formed, unreserved slug that no other server uses, and a scoping View
 * must be a dataHubView URN. A write that keeps a problem already stored is allowed, so an
 * unrelated global settings change still saves when older data breaks a rule.
 */
@Setter
@Getter
@Accessors(chain = true)
public class McpSettingsValidator extends AspectPayloadValidator {

  /**
   * Slugs reserved by the platform. "default", "datahub-default", and "datahub" denote the default
   * server (exposed at /mcp with no slug); "mcp" would collide with the mount path itself.
   */
  public static final Set<String> RESERVED_SLUGS =
      Set.of("default", "datahub-default", "datahub", "mcp");

  private static final Pattern SLUG_PATTERN = Pattern.compile("[a-z0-9][a-z0-9-]*");

  @Nonnull private AspectPluginConfig config;

  /** Why {@code slug} cannot identify a scoped MCP server, or null if it can. */
  @Nullable
  public static String slugError(@Nullable final String slug) {
    if (slug == null || slug.trim().isEmpty()) {
      return "MCP server config requires a non-empty slug (slug is required for scoped servers)";
    }
    if (!SLUG_PATTERN.matcher(slug).matches()) {
      return "MCP server slug must match [a-z0-9][a-z0-9-]* (got: '" + slug + "')";
    }
    if (RESERVED_SLUGS.contains(slug)) {
      return "MCP server slug '"
          + slug
          + "' is reserved; choose a different slug (reserved: "
          + RESERVED_SLUGS
          + ")";
    }
    return null;
  }

  /** Why {@code viewUrn} cannot scope an MCP server, or null if it can. */
  @Nullable
  public static String viewUrnError(@Nonnull final Urn viewUrn) {
    if (!DATAHUB_VIEW_ENTITY_NAME.equals(viewUrn.getEntityType())) {
      return "MCP server viewUrn must be a dataHubView URN (got entity type '"
          + viewUrn.getEntityType()
          + "')";
    }
    return null;
  }

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    // The stored settings, needed to allow problems they already hold, and the merged result of a
    // patch are only available at pre-commit.
    return Stream.empty();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    final ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    for (ChangeMCP item : changeMCPs) {
      final Set<String> problems = problems(mcpSettings(item.getAspect(GlobalSettingsInfo.class)));
      if (problems.isEmpty()) {
        continue;
      }
      if (item.getPreviousRecordTemplate() != null) {
        problems.removeAll(problems(mcpSettings(item.getPreviousAspect(GlobalSettingsInfo.class))));
      }
      problems.forEach(
          problem -> exceptions.addException(AspectValidationException.forItem(item, problem)));
    }

    return exceptions.streamAllExceptions();
  }

  @Nullable
  private static McpSettings mcpSettings(@Nullable final GlobalSettingsInfo info) {
    return info != null && info.hasMcpSettings() ? info.getMcpSettings() : null;
  }

  /** Every rule {@code settings} breaks, as messages that are equal for the same problem. */
  @Nonnull
  static Set<String> problems(@Nullable final McpSettings settings) {
    final Set<String> problems = new LinkedHashSet<>();
    if (settings == null) {
      return problems;
    }
    if (settings.hasServers()) {
      final Set<String> seenSlugs = new HashSet<>();
      for (McpServerConfig server : settings.getServers()) {
        final String slug = server.getSlug();
        final String slugError = slugError(slug);
        if (slugError != null) {
          problems.add(slugError);
        } else if (!seenSlugs.add(slug)) {
          problems.add("MCP server slug '" + slug + "' is used by more than one server");
        }
        addViewUrnProblem(server, "server '" + slug + "'", problems);
      }
    }
    if (settings.hasDefaultMcpServerOverrides()) {
      addViewUrnProblem(settings.getDefaultMcpServerOverrides(), "the default server", problems);
    }
    return problems;
  }

  private static void addViewUrnProblem(
      @Nonnull final McpServerConfig server,
      @Nonnull final String serverLabel,
      @Nonnull final Set<String> problems) {
    if (!server.hasViewUrn()) {
      return;
    }
    final String error = viewUrnError(server.getViewUrn());
    if (error != null) {
      problems.add(error + " on " + serverLabel + ": " + server.getViewUrn());
    }
  }
}

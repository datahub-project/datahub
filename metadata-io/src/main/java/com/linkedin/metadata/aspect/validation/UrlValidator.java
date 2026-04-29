package com.linkedin.metadata.aspect.validation;

import com.linkedin.common.url.Url;
import com.linkedin.identity.CorpGroupEditableInfo;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Validates URL fields (e.g. pictureLink) to prevent browser-side SSRF, credential harvesting, and
 * content injection attacks. Only HTTPS URLs pointing to public (non-internal) hosts are accepted.
 */
@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class UrlValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  private static final Set<String> ALLOWED_SCHEMES = Set.of("https");

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    for (BatchItem item : mcpItems) {
      String aspectName = item.getAspectName();

      if ("corpUserEditableInfo".equals(aspectName)) {
        CorpUserEditableInfo info = item.getAspect(CorpUserEditableInfo.class);
        if (info != null && info.hasPictureLink()) {
          validateUrl(item, info.getPictureLink(), "pictureLink", exceptions);
        }
      } else if ("corpGroupEditableInfo".equals(aspectName)) {
        CorpGroupEditableInfo info = item.getAspect(CorpGroupEditableInfo.class);
        if (info != null && info.hasPictureLink()) {
          validateUrl(item, info.getPictureLink(), "pictureLink", exceptions);
        }
      }
    }

    return exceptions.streamAllExceptions();
  }

  private void validateUrl(
      BatchItem item, Url url, String fieldName, ValidationExceptionCollection exceptions) {

    String rawUrl = url.toString();

    // Allow empty/blank values (user clearing their profile image)
    if (rawUrl == null || rawUrl.isBlank()) {
      return;
    }

    // Allow the default avatar relative path
    if (rawUrl.startsWith("assets/")) {
      return;
    }

    URI uri;
    try {
      uri = new URI(rawUrl);
    } catch (URISyntaxException e) {
      exceptions.addException(
          AspectValidationException.forItem(
              item, String.format("Invalid URL syntax for '%s': %s", fieldName, rawUrl)));
      return;
    }

    String scheme = uri.getScheme();
    if (scheme == null || !ALLOWED_SCHEMES.contains(scheme.toLowerCase())) {
      exceptions.addException(
          AspectValidationException.forItem(
              item,
              String.format(
                  "URL scheme '%s' is not allowed for '%s'. Only HTTPS URLs are accepted.",
                  scheme, fieldName)));
      return;
    }

    String host = uri.getHost();
    if (host == null || host.isEmpty()) {
      exceptions.addException(
          AspectValidationException.forItem(
              item, String.format("URL for '%s' must have a valid hostname.", fieldName)));
      return;
    }

    if (isInternalHost(host)) {
      exceptions.addException(
          AspectValidationException.forItem(
              item,
              String.format(
                  "URL for '%s' points to a private/internal network address, which is not allowed.",
                  fieldName)));
    }
  }

  /**
   * Checks whether a hostname resolves to a private, loopback, or link-local address, or uses
   * well-known internal hostnames (e.g. cloud metadata endpoints).
   */
  static boolean isInternalHost(String host) {
    // Block well-known cloud metadata IP
    if ("169.254.169.254".equals(host)) {
      return true;
    }

    // Block localhost variants
    if ("localhost".equalsIgnoreCase(host) || "127.0.0.1".equals(host) || "::1".equals(host)) {
      return true;
    }

    try {
      InetAddress address = InetAddress.getByName(host);
      return address.isLoopbackAddress()
          || address.isSiteLocalAddress()
          || address.isLinkLocalAddress()
          || address.isAnyLocalAddress();
    } catch (UnknownHostException e) {
      // If the host can't be resolved, reject it to be safe
      return true;
    }
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }
}

package auth.sso.oidc;

import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.metadata.IOidcOpMetadataResolver;
import org.pac4j.oidc.metadata.OidcOpMetadataResolver;

/**
 * Times discovery-document reads. Pac4j performs the read in {@link
 * OidcOpMetadataResolver#retrieveMetadata()}, both while the client is initialized and later when
 * {@code load()} decides the resource has changed.
 */
@Slf4j
class DiscoveryTimingOidcConfiguration extends OidcConfiguration {

  @Override
  protected IOidcOpMetadataResolver createNewOpMetadataResolver() {
    final String discoveryUri = getDiscoveryURI();
    if (discoveryUri != null && !discoveryUri.isBlank()) {
      final OidcOpMetadataResolver resolver = new TimedDiscoveryResolver(this);
      resolver.init();
      return resolver;
    }
    return super.createNewOpMetadataResolver();
  }

  private static final class TimedDiscoveryResolver extends OidcOpMetadataResolver {

    private TimedDiscoveryResolver(final OidcConfiguration configuration) {
      super(configuration);
    }

    @Override
    protected OIDCProviderMetadata retrieveMetadata() {
      final long startedAtNanos = System.nanoTime();
      final OIDCProviderMetadata metadata = super.retrieveMetadata();
      log.info(
          "OIDC provider metadata fetch completed in {} ms",
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos));
      return metadata;
    }
  }
}

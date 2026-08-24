package com.linkedin.gms.factory.common;

import io.ebean.config.DatabaseConfig;
import javax.annotation.Nonnull;

/**
 * Extension point for applying additional configuration to the primary Ebean {@link DatabaseConfig}
 * built by {@link LocalEbeanConfigFactory}. Every {@link EbeanConfigCustomizer} bean present in the
 * Spring context is applied, in an unspecified order, before the {@code Database} is created. OSS
 * registers none by default; an extension module may contribute one (e.g. to configure additional
 * database routing or connection settings).
 */
public interface EbeanConfigCustomizer {

  void customize(@Nonnull DatabaseConfig config);
}

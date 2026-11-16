package com.linkedin.gms.factory.common;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.gms.factory.ProductionConfigContextRunner;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import javax.net.ssl.SSLContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.testng.annotations.Test;

/**
 * Loads {@link ElasticsearchSSLContextFactory} with the production {@code application.yaml} and
 * checks that {@code elasticSearchSSLContext} is built from the primary cluster's {@code
 * sslContext} block only.
 */
public class ElasticsearchSSLContextFactoryTest {

  private static final String PRIMARY_SSL = "elasticsearch.clusters.primary.sslContext.";
  private static final String SECONDARY_SSL = "elasticsearch.clusters.secondary.sslContext.";

  private static ApplicationContextRunner runner() {
    return ProductionConfigContextRunner.create()
        .withUserConfiguration(ElasticsearchSSLContextFactory.class);
  }

  @Test
  public void testProductionDefaultsBuildADefaultTlsContext() {
    runner()
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(sslContext(context).getProtocol()).isEqualTo("TLS");
            });
  }

  @Test
  public void testPrimaryProtocolIsApplied() {
    runner()
        .withPropertyValues(PRIMARY_SSL + "protocol=TLSv1.2")
        .run(context -> assertThat(sslContext(context).getProtocol()).isEqualTo("TLSv1.2"));
  }

  @Test
  public void testSecondaryClusterSettingsDoNotAffectThePrimaryContext() {
    runner()
        .withPropertyValues(
            SECONDARY_SSL + "protocol=TLSv1.2",
            SECONDARY_SSL + "secureRandomImplementation=NoSuchAlgorithm")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(sslContext(context).getProtocol()).isEqualTo("TLS");
            });
  }

  @Test
  public void testUnknownSecureRandomFailsStartup() {
    runner()
        .withPropertyValues(PRIMARY_SSL + "secureRandomImplementation=NoSuchAlgorithm")
        .run(
            context ->
                assertThat(context)
                    .getFailure()
                    .hasRootCauseInstanceOf(NoSuchAlgorithmException.class));
  }

  /**
   * Covers that a configured truststore file is opened with its type and password. The store is
   * empty, so this does not check which certificates the resulting context trusts.
   */
  @Test
  public void testConfiguredTrustStoreIsOpenedWithItsPassword() throws Exception {
    Path trustStore = emptyPkcs12("changeit");
    try {
      runner()
          .withPropertyValues(
              PRIMARY_SSL + "trustStoreFile=" + trustStore,
              PRIMARY_SSL + "trustStoreType=PKCS12",
              PRIMARY_SSL + "trustStorePassword=changeit")
          .run(context -> assertThat(context).hasNotFailed());

      runner()
          .withPropertyValues(
              PRIMARY_SSL + "trustStoreFile=" + trustStore,
              PRIMARY_SSL + "trustStoreType=PKCS12",
              PRIMARY_SSL + "trustStorePassword=wrong-password")
          .run(
              context ->
                  assertThat(context)
                      .getFailure()
                      .hasRootCauseInstanceOf(UnrecoverableKeyException.class));
    } finally {
      Files.deleteIfExists(trustStore);
    }
  }

  @Test
  public void testIncompleteTrustStoreSettingsAreIgnored() {
    // Without a type and password the file is never opened, so a missing path does not matter.
    runner()
        .withPropertyValues(PRIMARY_SSL + "trustStoreFile=/does/not/exist.p12")
        .run(context -> assertThat(context).hasNotFailed());
  }

  @Test
  public void testMissingTrustStoreFileFailsStartup() {
    runner()
        .withPropertyValues(
            PRIMARY_SSL + "trustStoreFile=/does/not/exist.p12",
            PRIMARY_SSL + "trustStoreType=PKCS12",
            PRIMARY_SSL + "trustStorePassword=changeit")
        .run(
            context ->
                assertThat(context)
                    .getFailure()
                    .hasRootCauseInstanceOf(FileNotFoundException.class));
  }

  private static SSLContext sslContext(ApplicationContext applicationContext) {
    return applicationContext.getBean("elasticSearchSSLContext", SSLContext.class);
  }

  private static Path emptyPkcs12(String password) throws Exception {
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(null, null);
    Path file = Files.createTempFile("es-truststore", ".p12");
    try (OutputStream out = Files.newOutputStream(file)) {
      keyStore.store(out, password.toCharArray());
    }
    return file;
  }
}

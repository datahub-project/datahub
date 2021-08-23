package com.linkedin.gms.factory.common;

import org.apache.http.ssl.SSLContextBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;


@Configuration
public class ElasticsearchSSLContextFactory {

    @Value("${ELASTICSEARCH_SSL_PROTOCOL:#{null}}")
    private String sslProtocol;

    @Value("${ELASTICSEARCH_SSL_SECURE_RANDOM_IMPL:#{null}}")
    private String sslSecureRandomImplementation;

    @Value("${ELASTICSEARCH_SSL_TRUSTSTORE_FILE:#{null}}")
    private String sslTrustStoreFile;

    @Value("${ELASTICSEARCH_SSL_TRUSTSTORE_TYPE:#{null}}")
    private String sslTrustStoreType;

    @Value("${ELASTICSEARCH_SSL_TRUSTSTORE_PASSWORD:#{null}}")
    private String sslTrustStorePassword;

    @Value("${ELASTICSEARCH_SSL_KEYSTORE_FILE:#{null}}")
    private String sslKeyStoreFile;

    @Value("${ELASTICSEARCH_SSL_KEYSTORE_TYPE:#{null}}")
    private String sslKeyStoreType;

    @Value("${ELASTICSEARCH_SSL_KEYSTORE_PASSWORD:#{null}}")
    private String sslKeyStorePassword;

    @Value("${ELASTICSEARCH_SSL_KEY_PASSWORD:#{null}}")
    private String sslKeyPassword;

    @Bean(name = "elasticSearchSSLContext")
    public SSLContext createInstance() {
        final SSLContextBuilder sslContextBuilder = new SSLContextBuilder();
        if (sslProtocol != null) {
            sslContextBuilder.useProtocol(sslProtocol);
        }

        if (sslTrustStoreFile != null && sslTrustStoreType != null && sslTrustStorePassword != null) {
            loadTrustStore(sslContextBuilder, sslTrustStoreFile, sslTrustStoreType, sslTrustStorePassword);
        }

        if (sslKeyStoreFile != null && sslKeyStoreType != null && sslKeyStorePassword != null && sslKeyPassword != null) {
            loadKeyStore(sslContextBuilder, sslKeyStoreFile, sslKeyStoreType, sslKeyStorePassword, sslKeyPassword);
        }

        final SSLContext sslContext;
        try {
            if (sslSecureRandomImplementation != null) {
                sslContextBuilder.setSecureRandom(SecureRandom.getInstance(sslSecureRandomImplementation));
            }
            sslContext = sslContextBuilder.build();
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new RuntimeException("Failed to build SSL Context", e);
        }
        return sslContext;
    }

    private void loadKeyStore(@Nonnull SSLContextBuilder sslContextBuilder, @Nonnull String path,
                              @Nonnull String type, @Nonnull String password, @Nonnull String keyPassword) {
        try (InputStream identityFile = new FileInputStream(path)) {
            final KeyStore keystore = KeyStore.getInstance(type);
            keystore.load(identityFile, password.toCharArray());
            sslContextBuilder.loadKeyMaterial(keystore, keyPassword.toCharArray());
        } catch (IOException | CertificateException | NoSuchAlgorithmException | KeyStoreException | UnrecoverableKeyException e) {
            throw new RuntimeException("Failed to load key store: " + path, e);
        }
    }

    private void loadTrustStore(@Nonnull SSLContextBuilder sslContextBuilder, @Nonnull String path,
                                     @Nonnull String type, @Nonnull String password) {
        try (InputStream identityFile = new FileInputStream(path)) {
            final KeyStore keystore = KeyStore.getInstance(type);
            keystore.load(identityFile, password.toCharArray());
            sslContextBuilder.loadTrustMaterial(keystore, null);
        } catch (IOException | CertificateException | NoSuchAlgorithmException | KeyStoreException e) {
            throw new RuntimeException("Failed to load key store: " + path, e);
        }
    }

}

package controllers;

import auth.Authenticator;
import client.AuthServiceClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.mvc.Controller;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Security;
import utils.ConfigUtil;

import static auth.AuthUtils.*;


// TODO: Migrate this to metadata-service.
public class TrackingController extends Controller {

    private final Logger _logger = LoggerFactory.getLogger(TrackingController.class.getName());

    private static final List<String> KAFKA_SSL_PROTOCOLS = Collections.unmodifiableList(
        Arrays.asList(SecurityProtocol.SSL.name(), SecurityProtocol.SASL_SSL.name(),
            SecurityProtocol.SASL_PLAINTEXT.name()));

    private final Boolean _isEnabled;
    private final Config _config;
    private final KafkaProducer<String, String> _producer;
    private final String _topic;

    @Inject
    AuthServiceClient _authClient;

    @Inject
    public TrackingController(@Nonnull Config config) {
        _config = config;
        _isEnabled = !config.hasPath("analytics.enabled") || config.getBoolean("analytics.enabled");
        if (_isEnabled) {
            _logger.debug("Analytics tracking is enabled");
            _producer = createKafkaProducer();
            _topic = config.getString("analytics.tracking.topic");
        } else {
            _producer = null;
            _topic = null;
        }
    }

    @Security.Authenticated(Authenticator.class)
    @Nonnull
    public Result track(Http.Request request) throws Exception {
        if (!_isEnabled) {
            // If tracking is disabled, simply return a 200.
            return status(200);
        }

        JsonNode event;
        try {
            event = request.body().asJson();
        } catch (Exception e) {
            return badRequest();
        }
        final String actor = request.session().data().get(ACTOR);
        try {
            _logger.debug(String.format("Emitting product analytics event. actor: %s, event: %s", actor, event));
            final ProducerRecord<String, String> record = new ProducerRecord<>(
                _topic,
                actor,
                event.toString());
            _producer.send(record);
            _producer.flush();
            _authClient.track(event.toString());
            return ok();
        } catch (Exception e) {
            _logger.error(String.format("Failed to emit product analytics event. actor: %s, event: %s", actor, event));
            return internalServerError(e.getMessage());
        }
    }

    @Override
    protected void finalize() {
        _producer.close();
    }

    private void setConfig(Properties props, String key, String configKey) {
        Optional.ofNullable(ConfigUtil.getString(_config, configKey, null))
            .ifPresent(v -> props.put(key, v));
    }

    private KafkaProducer createKafkaProducer() {
        final Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "datahub-frontend");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _config.getString("analytics.kafka.bootstrap.server"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"); // Actor urn.
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"); // JSON object.

        final String securityProtocolConfig = "analytics.kafka.security.protocol";
        if (_config.hasPath(securityProtocolConfig)
                && KAFKA_SSL_PROTOCOLS.contains(_config.getString(securityProtocolConfig))) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, _config.getString(securityProtocolConfig));
            setConfig(props, SslConfigs.SSL_KEY_PASSWORD_CONFIG, "analytics.kafka.ssl.key.password");

            setConfig(props, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "analytics.kafka.ssl.keystore.type");
            setConfig(props, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "analytics.kafka.ssl.keystore.location");
            setConfig(props, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "analytics.kafka.ssl.keystore.password");

            setConfig(props, SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "analytics.kafka.ssl.truststore.type");
            setConfig(props, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "analytics.kafka.ssl.truststore.location");
            setConfig(props, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "analytics.kafka.ssl.truststore.password");

            setConfig(props, SslConfigs.SSL_PROTOCOL_CONFIG, "analytics.kafka.ssl.protocol");
            setConfig(props, SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "analytics.kafka.ssl.endpoint.identification.algorithm");

            final String securityProtocol = _config.getString(securityProtocolConfig);
            if (securityProtocol.equals(SecurityProtocol.SASL_SSL.name())
                    || securityProtocol.equals(SecurityProtocol.SASL_PLAINTEXT.name())) {
                setConfig(props, SaslConfigs.SASL_MECHANISM, "analytics.kafka.sasl.mechanism");
                setConfig(props, SaslConfigs.SASL_JAAS_CONFIG, "analytics.kafka.sasl.jaas.config");
                setConfig(props, SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "analytics.kafka.sasl.kerberos.service.name");
                setConfig(props, SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "analytics.kafka.sasl.login.callback.handler.class");
            }
        }

        return new KafkaProducer(props);
    }
}

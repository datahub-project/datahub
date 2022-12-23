package io.datahubproject.openapi.schema.registry;
/*
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.kafka.schemaregistry.InternalSchemaRegistryFactory;
import com.linkedin.metadata.schema.registry.SchemaRegistryService;
import io.datahubproject.schema_registry.openapi.generated.RegisterSchemaRequest;
import io.datahubproject.schema_registry.openapi.generated.RegisterSchemaResponse;
import io.datahubproject.schema_registry.openapi.generated.SchemaString;
import io.swagger.api.CompatibilityApi;
import io.swagger.api.ConfigApi;
import io.swagger.api.ContextsApi;
import io.swagger.api.DefaultApi;
import io.swagger.api.ModeApi;
import io.swagger.api.SchemasApi;
import io.swagger.api.SubjectsApi;
import io.swagger.api.V1Api;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
*/

/**
 * DataHub Rest Controller implementation for Confluent's Schema Registry OpenAPI spec.
 */
/*
@Slf4j
@RestController
@RequestMapping("/schema-registry")
@RequiredArgsConstructor
@ConditionalOnProperty(name = "kafka.schemaRegistry.type", havingValue = InternalSchemaRegistryFactory.TYPE)
public class SubjectsAPIController implements SubjectsApi {

  private final ObjectMapper objectMapper;

  private final HttpServletRequest request;

  @Qualifier("schemaRegistryService")
  private final SchemaRegistryService _schemaRegistryService;

  @Override
  public Optional<ObjectMapper> getObjectMapper() {
    return Optional.ofNullable(objectMapper);
  }

  @Override
  public Optional<HttpServletRequest> getRequest() {
    return Optional.ofNullable(request);
  }

  @Override
  public ResponseEntity<RegisterSchemaResponse> register(String subject, RegisterSchemaRequest body,
      Boolean normalize) {
    final String topicName = subject.replaceFirst("-value", "");
    return _schemaRegistryService.getSchemaIdForTopic(topicName).map(id -> {
      final RegisterSchemaResponse response = new RegisterSchemaResponse();
      return new ResponseEntity<>(response.id(id), HttpStatus.OK);
    }).orElseGet(() -> {
      log.error("Couldn't find topic with name {}.", topicName);
      return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    });
  }


}
*/
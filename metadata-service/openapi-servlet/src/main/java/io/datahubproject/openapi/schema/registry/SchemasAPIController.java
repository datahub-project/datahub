package io.datahubproject.openapi.schema.registry;
/*
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.kafka.schemaregistry.InternalSchemaRegistryFactory;
import com.linkedin.metadata.schema.registry.SchemaRegistryService;
import io.datahubproject.schema_registry.openapi.generated.SchemaString;
import io.swagger.api.SchemasApi;
import io.swagger.v3.oas.annotations.Hidden;
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

@Slf4j
@RestController
@RequestMapping("/schema-registry")
@RequiredArgsConstructor
@ConditionalOnProperty(name = "kafka.schemaRegistry.type", havingValue = InternalSchemaRegistryFactory.TYPE)
public class SchemasAPIController implements SchemasApi {

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
  public ResponseEntity<SchemaString> getSchema(Integer id, String subject, String format, Boolean fetchMaxId) {
    return _schemaRegistryService.getSchemaForId(id).map(schema -> {
      SchemaString result = new SchemaString();
      result.setMaxId(id);
      result.setSchemaType("AVRO");
      result.setSchema(schema.toString());
      return new ResponseEntity<>(result, HttpStatus.OK);
    }).orElseGet(() -> {
      log.error("Couldn't find topic with id {}.", id);
      return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    });
  }
}
*/
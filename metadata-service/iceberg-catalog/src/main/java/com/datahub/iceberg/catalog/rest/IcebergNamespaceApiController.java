package com.datahub.iceberg.catalog.rest;

import static com.datahub.iceberg.catalog.Utils.*;

import com.datahub.iceberg.catalog.DataOperation;
import jakarta.servlet.http.HttpServletRequest;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
public class IcebergNamespaceApiController extends AbstractIcebergController {

  @GetMapping(
      value = "/v1/{prefix}/namespaces/{namespace}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  public GetNamespaceResponse getNamespace(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @PathVariable("namespace") String namespace) {
    log.info("GET NAMESPACE REQUEST ns {}", namespace);

    GetNamespaceResponse getNamespaceResponse =
        catalogOperation(
            platformInstance,
            request,
            operationContext ->
                authorize(
                    operationContext, platformInstance, DataOperation.MANAGE_NAMESPACES, false),
            catalog -> {
              // not supporting properties; simply load to ensure existence
              Namespace ns = namespaceFromString(namespace);
              catalog.loadNamespaceMetadata(ns);
              return GetNamespaceResponse.builder().withNamespace(ns).build();
            },
            null);

    log.info("GET NAMESPACE RESPONSE {}", getNamespaceResponse);
    return getNamespaceResponse;
  }

  @PostMapping(
      value = "/v1/{prefix}/namespaces",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public CreateNamespaceResponse createNamespace(
      HttpServletRequest request,
      @RequestBody @Nonnull CreateNamespaceRequest createNamespaceRequest,
      @PathVariable("prefix") String platformInstance) {
    log.info("CREATE NAMESPACE REQUEST {} ", createNamespaceRequest);

    CreateNamespaceResponse createNamespaceResponse =
        catalogOperation(
            platformInstance,
            request,
            operationContext ->
                authorize(
                    operationContext, platformInstance, DataOperation.MANAGE_NAMESPACES, false),
            catalog -> {
              catalog.createNamespace(createNamespaceRequest.namespace());
              return CreateNamespaceResponse.builder()
                  .withNamespace(createNamespaceRequest.namespace())
                  .build();
            },
            null);

    log.info("CREATE NAMESPACE RESPONSE {}", createNamespaceResponse);
    return createNamespaceResponse;
  }
}

package io.datahubproject.openapi.util;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static io.datahubproject.openapi.util.ReflectionCache.toUpperFirst;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.avro2pegasus.events.KafkaAuditHeader;
import com.linkedin.avro2pegasus.events.UUID;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.ByteString;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.entity.ebean.transactions.AspectsBatchImpl;
import com.linkedin.metadata.entity.transactions.AspectsBatch;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.openapi.dto.RollbackRunResultDto;
import io.datahubproject.openapi.dto.UpsertAspectRequest;
import io.datahubproject.openapi.generated.AspectRowSummary;
import io.datahubproject.openapi.generated.AspectType;
import io.datahubproject.openapi.generated.AuditStamp;
import io.datahubproject.openapi.generated.EntityResponse;
import io.datahubproject.openapi.generated.EnvelopedAspect;
import io.datahubproject.openapi.generated.MetadataChangeProposal;
import io.datahubproject.openapi.generated.OneOfEnvelopedAspectValue;
import io.datahubproject.openapi.generated.OneOfGenericAspectValue;
import io.datahubproject.openapi.generated.Status;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.client.HttpClientErrorException;

@Slf4j
public class MappingUtil {
  private MappingUtil() {}

  private static final JsonNodeFactory NODE_FACTORY = JsonNodeFactory.instance;
  private static final Map<String, Class<? extends OneOfEnvelopedAspectValue>>
      ENVELOPED_ASPECT_TYPE_MAP = new HashMap<>();
  private static final Map<Class<? extends OneOfGenericAspectValue>, String> ASPECT_NAME_MAP =
      new HashMap<>();
  private static final Map<String, Class<? extends RecordTemplate>> PEGASUS_TYPE_MAP =
      new HashMap<>();

  private static final String DISCRIMINATOR = "__type";
  private static final String PEGASUS_PACKAGE = "com.linkedin";
  private static final String OPENAPI_PACKAGE = "io.datahubproject.openapi.generated";
  private static final ReflectionCache REFLECT_AVRO =
      ReflectionCache.builder().basePackage("com.linkedin.pegasus2avro").build();
  private static final ReflectionCache REFLECT_OPENAPI =
      ReflectionCache.builder().basePackage(OPENAPI_PACKAGE).build();

  static {
    // Build a map from __type name to generated class
    ClassPathScanningCandidateComponentProvider provider =
        new ClassPathScanningCandidateComponentProvider(false);
    provider.addIncludeFilter(new AssignableTypeFilter(OneOfEnvelopedAspectValue.class));
    Set<BeanDefinition> components =
        provider.findCandidateComponents("io/datahubproject/openapi/generated");
    components.forEach(MappingUtil::putEnvelopedAspectEntry);

    provider = new ClassPathScanningCandidateComponentProvider(false);
    provider.addIncludeFilter(new AssignableTypeFilter(OneOfGenericAspectValue.class));
    components = provider.findCandidateComponents("io/datahubproject/openapi/generated");
    components.forEach(MappingUtil::putGenericAspectEntry);

    // Build a map from fully qualified Pegasus generated class name to class
    new Reflections(PEGASUS_PACKAGE, new SubTypesScanner(false))
        .getSubTypesOf(RecordTemplate.class)
        .forEach(aClass -> PEGASUS_TYPE_MAP.put(aClass.getSimpleName(), aClass));
  }

  public static Map<String, EntityResponse> mapServiceResponse(
      Map<Urn, com.linkedin.entity.EntityResponse> serviceResponse, ObjectMapper objectMapper) {
    return serviceResponse.entrySet().stream()
        .collect(
            Collectors.toMap(
                entry -> entry.getKey().toString(),
                entry -> mapEntityResponse(entry.getValue(), objectMapper)));
  }

  public static EntityResponse mapEntityResponse(
      com.linkedin.entity.EntityResponse entityResponse, ObjectMapper objectMapper) {
    return EntityResponse.builder()
        .entityName(entityResponse.getEntityName())
        .urn(entityResponse.getUrn().toString())
        .aspects(
            entityResponse.getAspects().entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> mapEnvelopedAspect(entry.getValue(), objectMapper))))
        .build();
  }

  public static EnvelopedAspect mapEnvelopedAspect(
      com.linkedin.entity.EnvelopedAspect envelopedAspect, ObjectMapper objectMapper) {
    return EnvelopedAspect.builder()
        .name(envelopedAspect.getName())
        .timestamp(envelopedAspect.getTimestamp())
        .version(envelopedAspect.getVersion())
        .type(AspectType.fromValue(envelopedAspect.getType().name().toUpperCase(Locale.ROOT)))
        .created(objectMapper.convertValue(envelopedAspect.getCreated().data(), AuditStamp.class))
        .value(mapAspectValue(envelopedAspect.getName(), envelopedAspect.getValue(), objectMapper))
        .build();
  }

  private static DataMap insertDiscriminator(@Nullable Class<?> parentClazz, DataMap dataMap) {
    if (parentClazz != null && REFLECT_OPENAPI.lookupMethod(parentClazz, "get__type") != null) {
      dataMap.put(DISCRIMINATOR, parentClazz.getSimpleName());
    }

    Set<Map.Entry<String, DataMap>> requiresDiscriminator =
        dataMap.entrySet().stream()
            .filter(e -> e.getValue() instanceof DataMap)
            .filter(e -> shouldCollapseClassToDiscriminator(e.getKey()))
            .map(e -> Map.entry(e.getKey(), (DataMap) e.getValue()))
            .collect(Collectors.toSet());
    // DataMap doesn't support concurrent access
    requiresDiscriminator.forEach(
        e -> {
          dataMap.remove(e.getKey());
          dataMap.put(DISCRIMINATOR, e.getKey().substring(e.getKey().lastIndexOf(".") + 1));
          dataMap.putAll(e.getValue());
        });

    // Look through all the nested classes for possible discriminator requirements
    Set<Pair<List<String>, DataMap>> nestedDataMaps =
        getDataMapPaths(new LinkedList<>(), dataMap).collect(Collectors.toSet());
    // DataMap doesn't support concurrent access
    for (Pair<List<String>, DataMap> nestedDataMapPath : nestedDataMaps) {
      List<String> nestedPath = nestedDataMapPath.getFirst();
      DataMap nested = nestedDataMapPath.getSecond();
      Class<?> nextClazz = parentClazz;

      if (nextClazz != null) {
        // reconstruct type path from method path
        for (String pathElem : nestedPath) {
          // if not list element
          if (!pathElem.startsWith("[") && !pathElem.contains(".")) {
            String methodName = "get" + toUpperFirst(pathElem);
            Method getMethod = REFLECT_OPENAPI.lookupMethod(nextClazz, methodName);
            nextClazz = getMethod != null ? getMethod.getReturnType() : null;

            if (nextClazz != null && "List".equals(nextClazz.getSimpleName())) {
              String listElemClassName =
                  getMethod
                      .getGenericReturnType()
                      .getTypeName()
                      .replace("java.util.List<", "")
                      .replace(">", "");
              try {
                nextClazz = Class.forName(listElemClassName);
              } catch (ClassNotFoundException ex) {
                log.warn("Class lookup failed for {}", listElemClassName);
                nextClazz = null;
              }
            }
          }
        }

        if ((nextClazz != parentClazz && shouldCheckTypeMethod(nextClazz))
            || nested.keySet().stream().anyMatch(MappingUtil::shouldCollapseClassToDiscriminator)) {
          insertDiscriminator(nextClazz, nested);
        }
      }
    }

    return dataMap;
  }

  /**
   * Stream paths to DataMaps
   *
   * @param paths current path
   * @param data current DataMap or DataList
   * @return path to all nested DataMaps
   */
  private static Stream<Pair<List<String>, DataMap>> getDataMapPaths(
      List<String> paths, Object data) {
    if (data instanceof DataMap) {
      return ((DataMap) data)
          .entrySet().stream()
              .filter(e -> e.getValue() instanceof DataMap || e.getValue() instanceof DataList)
              .flatMap(
                  entry -> {
                    List<String> thisPath = new LinkedList<>(paths);
                    thisPath.add(entry.getKey());
                    if (entry.getValue() instanceof DataMap) {
                      return Stream.concat(
                          Stream.of(Pair.of(thisPath, (DataMap) entry.getValue())),
                          getDataMapPaths(thisPath, entry.getValue()));
                    } else {
                      // DataList
                      return getDataMapPaths(thisPath, entry.getValue());
                    }
                  });
    } else if (data instanceof DataList) {
      DataList dataList = (DataList) data;
      return IntStream.range(0, dataList.size())
          .mapToObj(idx -> Pair.of(idx, dataList.get(idx)))
          .filter(
              idxObject ->
                  idxObject.getValue() instanceof DataMap
                      || idxObject.getValue() instanceof DataList)
          .flatMap(
              idxObject -> {
                Object item = idxObject.getValue();
                List<String> thisPath = new LinkedList<>(paths);
                thisPath.add("[" + idxObject.getKey() + "]");
                if (item instanceof DataMap) {
                  return Stream.concat(
                      Stream.of(Pair.of(thisPath, (DataMap) item)),
                      getDataMapPaths(thisPath, item));
                } else {
                  // DataList
                  return getDataMapPaths(thisPath, item);
                }
              });
    }
    return Stream.empty();
  }

  public static OneOfEnvelopedAspectValue mapAspectValue(
      String aspectName, Aspect aspect, ObjectMapper objectMapper) {
    Class<? extends OneOfEnvelopedAspectValue> aspectClass =
        ENVELOPED_ASPECT_TYPE_MAP.get(aspectName);
    DataMap wrapper = insertDiscriminator(aspectClass, aspect.data());
    try {
      String dataMapAsJson = objectMapper.writeValueAsString(wrapper);
      return objectMapper.readValue(dataMapAsJson, aspectClass);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static void putEnvelopedAspectEntry(BeanDefinition beanDefinition) {
    try {
      Class<? extends OneOfEnvelopedAspectValue> cls =
          (Class<? extends OneOfEnvelopedAspectValue>)
              Class.forName(beanDefinition.getBeanClassName());
      String aspectName = getAspectName(cls);
      ENVELOPED_ASPECT_TYPE_MAP.put(aspectName, cls);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static void putGenericAspectEntry(BeanDefinition beanDefinition) {
    try {
      Class<? extends OneOfGenericAspectValue> cls =
          (Class<? extends OneOfGenericAspectValue>)
              Class.forName(beanDefinition.getBeanClassName());
      String aspectName = getAspectName(cls);
      ASPECT_NAME_MAP.put(cls, aspectName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static String getAspectName(Class<?> cls) {
    char[] c = cls.getSimpleName().toCharArray();
    c[0] = Character.toLowerCase(c[0]);
    return new String(c);
  }

  private static boolean shouldCheckTypeMethod(@Nullable Class<?> parentClazz) {
    return Optional.ofNullable(parentClazz)
        .map(cls -> cls.getName().startsWith(OPENAPI_PACKAGE + "."))
        .orElse(false);
  }

  private static boolean shouldCollapseClassToDiscriminator(String className) {
    return className.startsWith(PEGASUS_PACKAGE + ".");
  }

  private static Optional<String> shouldDiscriminate(
      String parentShortClass, String fieldName, ObjectNode node) {
    try {
      if (parentShortClass != null) {
        Class<?> pegasus2AvroClazz = REFLECT_AVRO.lookupClass(parentShortClass, true);
        Method getClassSchema = REFLECT_AVRO.lookupMethod(pegasus2AvroClazz, "getClassSchema");
        Schema avroSchema = (Schema) getClassSchema.invoke(null);
        Schema.Field avroField = avroSchema.getField(fieldName);

        if (avroField.schema().isUnion()) {
          Class<?> discriminatedClazz =
              REFLECT_AVRO.lookupClass(node.get(DISCRIMINATOR).asText(), true);
          return Optional.of(discriminatedClazz.getName().replace(".pegasus2avro", ""));
        }
      }

      // check leaf
      Iterator<String> itr = node.fieldNames();
      itr.next();
      if (!itr.hasNext()) { // only contains discriminator
        Class<?> discriminatedClazz =
            REFLECT_AVRO.lookupClass(node.get(DISCRIMINATOR).asText(), true);
        return Optional.of(discriminatedClazz.getName().replace(".pegasus2avro", ""));
      }

    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
    return Optional.empty();
  }

  private static void replaceDiscriminator(ObjectNode node) {
    replaceDiscriminator(null, null, null, node);
  }

  private static void replaceDiscriminator(
      @Nullable ObjectNode parentNode,
      @Nullable String parentDiscriminator,
      @Nullable String propertyName,
      @Nonnull ObjectNode node) {

    final String discriminator;
    if (node.isObject() && node.has(DISCRIMINATOR)) {
      Optional<String> discriminatorClassName =
          shouldDiscriminate(parentDiscriminator, propertyName, node);
      if (parentNode != null && discriminatorClassName.isPresent()) {
        discriminator = node.remove(DISCRIMINATOR).asText();
        parentNode.remove(propertyName);
        parentNode.set(
            propertyName, NODE_FACTORY.objectNode().set(discriminatorClassName.get(), node));
      } else {
        discriminator = node.remove(DISCRIMINATOR).asText();
      }
    } else {
      discriminator = null;
    }

    List<Map.Entry<String, JsonNode>> objectChildren = new LinkedList<>();
    node.fields()
        .forEachRemaining(
            entry -> {
              if (entry.getValue().isObject()) {
                objectChildren.add(entry);
              } else if (entry.getValue().isArray()) {
                entry
                    .getValue()
                    .forEach(
                        i -> {
                          if (i.isObject()) {
                            objectChildren.add(Map.entry(entry.getKey(), i));
                          }
                        });
              }
            });

    objectChildren.forEach(
        entry ->
            replaceDiscriminator(
                node, discriminator, entry.getKey(), (ObjectNode) entry.getValue()));
  }

  @Nonnull
  public static GenericAspect convertGenericAspect(
      @Nonnull io.datahubproject.openapi.generated.GenericAspect genericAspect,
      ObjectMapper objectMapper) {
    try {
      ObjectNode jsonTree = (ObjectNode) objectMapper.valueToTree(genericAspect).get("value");
      replaceDiscriminator(jsonTree);
      String pretty = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonTree);
      return new GenericAspect()
          .setContentType(genericAspect.getContentType())
          .setValue(ByteString.copyString(pretty, UTF_8));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean authorizeProposals(
      List<com.linkedin.mxe.MetadataChangeProposal> proposals,
      EntityService entityService,
      Authorizer authorizer,
      String actorUrnStr,
      DisjunctivePrivilegeGroup orGroup) {
    List<Optional<EntitySpec>> resourceSpecs =
        proposals.stream()
            .map(
                proposal -> {
                  com.linkedin.metadata.models.EntitySpec entitySpec =
                      entityService.getEntityRegistry().getEntitySpec(proposal.getEntityType());
                  Urn entityUrn =
                      EntityKeyUtils.getUrnFromProposal(proposal, entitySpec.getKeyAspectSpec());
                  return Optional.of(
                      new EntitySpec(proposal.getEntityType(), entityUrn.toString()));
                })
            .collect(Collectors.toList());
    return AuthUtil.isAuthorizedForResources(authorizer, actorUrnStr, resourceSpecs, orGroup);
  }

  public static Pair<String, Boolean> ingestProposal(
      com.linkedin.mxe.MetadataChangeProposal serviceProposal,
      String actorUrn,
      EntityService entityService) {
    // TODO: Use the actor present in the IC.
    Timer.Context context = MetricUtils.timer("postEntity").time();
    final com.linkedin.common.AuditStamp auditStamp =
        new com.linkedin.common.AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(UrnUtils.getUrn(actorUrn));

    final List<com.linkedin.mxe.MetadataChangeProposal> additionalChanges =
        AspectUtils.getAdditionalChanges(serviceProposal, entityService);

    log.info("Proposal: {}", serviceProposal);
    Throwable exceptionally = null;
    try {
      Stream<com.linkedin.mxe.MetadataChangeProposal> proposalStream =
          Stream.concat(
              Stream.of(serviceProposal),
              AspectUtils.getAdditionalChanges(serviceProposal, entityService).stream());

      AspectsBatch batch =
          AspectsBatchImpl.builder()
              .mcps(proposalStream.collect(Collectors.toList()), entityService.getEntityRegistry())
              .build();

      Set<IngestResult> proposalResult = entityService.ingestProposal(batch, auditStamp, false);

      Urn urn = proposalResult.stream().findFirst().get().getUrn();
      return new Pair<>(
          urn.toString(), proposalResult.stream().anyMatch(IngestResult::isSqlCommitted));
    } catch (ValidationException ve) {
      exceptionally = ve;
      throw HttpClientErrorException.create(
          HttpStatus.UNPROCESSABLE_ENTITY, ve.getMessage(), null, null, null);
    } catch (Exception e) {
      exceptionally = e;
      throw e;
    } finally {
      if (exceptionally != null) {
        MetricUtils.counter(MetricRegistry.name("postEntity", "failed")).inc();
      } else {
        MetricUtils.counter(MetricRegistry.name("postEntity", "success")).inc();
      }
      context.stop();
    }
  }

  public static MetadataChangeProposal mapToProposal(UpsertAspectRequest aspectRequest) {
    MetadataChangeProposal.MetadataChangeProposalBuilder metadataChangeProposal =
        MetadataChangeProposal.builder();
    io.datahubproject.openapi.generated.GenericAspect genericAspect =
        io.datahubproject.openapi.generated.GenericAspect.builder()
            .value(aspectRequest.getAspect())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .build();
    io.datahubproject.openapi.generated.GenericAspect keyAspect = null;
    if (aspectRequest.getEntityKeyAspect() != null) {
      keyAspect =
          io.datahubproject.openapi.generated.GenericAspect.builder()
              .contentType(MediaType.APPLICATION_JSON_VALUE)
              .value(aspectRequest.getEntityKeyAspect())
              .build();
    }
    metadataChangeProposal
        .aspect(genericAspect)
        .changeType(io.datahubproject.openapi.generated.ChangeType.UPSERT)
        .aspectName(ASPECT_NAME_MAP.get(aspectRequest.getAspect().getClass()))
        .entityKeyAspect(keyAspect)
        .entityUrn(aspectRequest.getEntityUrn())
        .entityType(aspectRequest.getEntityType());

    return metadataChangeProposal.build();
  }

  public static com.linkedin.mxe.MetadataChangeProposal mapToServiceProposal(
      MetadataChangeProposal metadataChangeProposal, ObjectMapper objectMapper) {
    io.datahubproject.openapi.generated.KafkaAuditHeader auditHeader =
        metadataChangeProposal.getAuditHeader();

    com.linkedin.mxe.MetadataChangeProposal serviceProposal =
        new com.linkedin.mxe.MetadataChangeProposal()
            .setEntityType(metadataChangeProposal.getEntityType())
            .setChangeType(ChangeType.valueOf(metadataChangeProposal.getChangeType().name()));
    if (metadataChangeProposal.getEntityUrn() != null) {
      serviceProposal.setEntityUrn(UrnUtils.getUrn(metadataChangeProposal.getEntityUrn()));
    }
    if (metadataChangeProposal.getSystemMetadata() != null) {
      serviceProposal.setSystemMetadata(
          objectMapper.convertValue(
              metadataChangeProposal.getSystemMetadata(), SystemMetadata.class));
    }
    if (metadataChangeProposal.getAspectName() != null) {
      serviceProposal.setAspectName(metadataChangeProposal.getAspectName());
    }

    if (auditHeader != null) {
      KafkaAuditHeader kafkaAuditHeader = new KafkaAuditHeader();
      kafkaAuditHeader
          .setAuditVersion(auditHeader.getAuditVersion())
          .setTime(auditHeader.getTime())
          .setAppName(auditHeader.getAppName())
          .setMessageId(new UUID(ByteString.copyString(auditHeader.getMessageId(), UTF_8)))
          .setServer(auditHeader.getServer());
      if (auditHeader.getInstance() != null) {
        kafkaAuditHeader.setInstance(auditHeader.getInstance());
      }
      if (auditHeader.getAuditVersion() != null) {
        kafkaAuditHeader.setAuditVersion(auditHeader.getAuditVersion());
      }
      if (auditHeader.getFabricUrn() != null) {
        kafkaAuditHeader.setFabricUrn(auditHeader.getFabricUrn());
      }
      if (auditHeader.getClusterConnectionString() != null) {
        kafkaAuditHeader.setClusterConnectionString(auditHeader.getClusterConnectionString());
      }
      serviceProposal.setAuditHeader(kafkaAuditHeader);
    }

    serviceProposal =
        metadataChangeProposal.getEntityKeyAspect() != null
            ? serviceProposal.setEntityKeyAspect(
                MappingUtil.convertGenericAspect(
                    metadataChangeProposal.getEntityKeyAspect(), objectMapper))
            : serviceProposal;
    serviceProposal =
        metadataChangeProposal.getAspect() != null
            ? serviceProposal.setAspect(
                MappingUtil.convertGenericAspect(metadataChangeProposal.getAspect(), objectMapper))
            : serviceProposal;
    return serviceProposal;
  }

  public static RollbackRunResultDto mapRollbackRunResult(
      RollbackRunResult rollbackRunResult, ObjectMapper objectMapper) {
    List<AspectRowSummary> aspectRowSummaries =
        rollbackRunResult.getRowsRolledBack().stream()
            .map(
                aspectRowSummary ->
                    objectMapper.convertValue(aspectRowSummary.data(), AspectRowSummary.class))
            .collect(Collectors.toList());
    return RollbackRunResultDto.builder()
        .rowsRolledBack(aspectRowSummaries)
        .rowsDeletedFromEntityDeletion(rollbackRunResult.getRowsDeletedFromEntityDeletion())
        .build();
  }

  public static UpsertAspectRequest createStatusRemoval(Urn urn, EntityService entityService) {
    com.linkedin.metadata.models.EntitySpec entitySpec =
        entityService.getEntityRegistry().getEntitySpec(urn.getEntityType());
    if (entitySpec == null || !entitySpec.getAspectSpecMap().containsKey(STATUS_ASPECT_NAME)) {
      throw new IllegalArgumentException(
          "Entity type is not valid for soft deletes: " + urn.getEntityType());
    }
    return UpsertAspectRequest.builder()
        .aspect(Status.builder().removed(true).build())
        .entityUrn(urn.toString())
        .entityType(urn.getEntityType())
        .build();
  }
}

package io.datahubproject.openapi.util;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.avro2pegasus.events.KafkaAuditHeader;
import com.linkedin.avro2pegasus.events.UUID;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.ByteString;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.entity.ValidationException;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.resources.entity.AspectUtils;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.client.HttpClientErrorException;

import static com.linkedin.metadata.Constants.*;
import static java.nio.charset.StandardCharsets.*;

@Slf4j
public class MappingUtil {
  private MappingUtil() {

  }

  private static final Map<String, Class<? extends OneOfEnvelopedAspectValue>> ENVELOPED_ASPECT_TYPE_MAP =
      new HashMap<>();
  private static final Map<Class<? extends OneOfGenericAspectValue>, String> ASPECT_NAME_MAP =
      new HashMap<>();
  private static final Map<String, Class<? extends RecordTemplate>> PEGASUS_TYPE_MAP = new HashMap<>();
  private static final Pattern CLASS_NAME_PATTERN =
      Pattern.compile("(\"com\\.linkedin\\.)([a-z]+?\\.)+?(?<className>[A-Z]\\w+?)(\":\\{)(?<content>.*?)(}})");
  private static final Pattern GLOBAL_TAGS_PATTERN =
      Pattern.compile("\"globalTags\":\\{");
  private static final Pattern GLOSSARY_TERMS_PATTERN =
      Pattern.compile("\"glossaryTerms\":\\{");

  private static final String DISCRIMINATOR = "__type";
  private static final Pattern CLASS_TYPE_NAME_PATTERN =
      Pattern.compile("(\\s+?\"__type\"\\s+?:\\s+?\")(?<classTypeName>\\w*?)(\"[,]?\\s+?)(?<content>[\\S\\s]*?)(\\s+})");
  private static final String PEGASUS_PACKAGE = "com.linkedin";
  private static final String GLOBAL_TAGS = "GlobalTags";
  private static final String GLOSSARY_TERMS = "GlossaryTerms";

  static {
    // Build a map from __type name to generated class
    ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false);
    provider.addIncludeFilter(new AssignableTypeFilter(OneOfEnvelopedAspectValue.class));
    Set<BeanDefinition> components = provider.findCandidateComponents("io/datahubproject/openapi/generated");
    components.forEach(MappingUtil::putEnvelopedAspectEntry);

    provider = new ClassPathScanningCandidateComponentProvider(false);
    provider.addIncludeFilter(new AssignableTypeFilter(OneOfGenericAspectValue.class));
    components = provider.findCandidateComponents("io/datahubproject/openapi/generated");
    components.forEach(MappingUtil::putGenericAspectEntry);

    List<ClassLoader> classLoadersList = new ArrayList<>();
    classLoadersList.add(ClasspathHelper.contextClassLoader());
    classLoadersList.add(ClasspathHelper.staticClassLoader());

    // Build a map from fully qualified Pegasus generated class name to class
    Reflections reflections = new Reflections(new ConfigurationBuilder()
        .setScanners(new SubTypesScanner(false), new ResourcesScanner())
        .setUrls(ClasspathHelper.forClassLoader(classLoadersList.toArray(new ClassLoader[0])))
        .filterInputsBy(new FilterBuilder().include(FilterBuilder.prefix(PEGASUS_PACKAGE))));
    Set<Class<? extends RecordTemplate>> pegasusComponents = reflections.getSubTypesOf(RecordTemplate.class);
    pegasusComponents.forEach(aClass -> PEGASUS_TYPE_MAP.put(aClass.getSimpleName(), aClass));
  }

  public static Map<String, EntityResponse> mapServiceResponse(Map<Urn, com.linkedin.entity.EntityResponse> serviceResponse,
      ObjectMapper objectMapper) {
    return serviceResponse.entrySet()
        .stream()
        .collect(Collectors.toMap(entry -> entry.getKey().toString(), entry -> mapEntityResponse(entry.getValue(), objectMapper)));
  }

  public static EntityResponse mapEntityResponse(com.linkedin.entity.EntityResponse entityResponse, ObjectMapper objectMapper) {
    return new EntityResponse().entityName(entityResponse.getEntityName())
        .urn(entityResponse.getUrn().toString())
        .aspects(entityResponse.getAspects()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> mapEnvelopedAspect(entry.getValue(), objectMapper))));
  }

  public static EnvelopedAspect mapEnvelopedAspect(com.linkedin.entity.EnvelopedAspect envelopedAspect,
      ObjectMapper objectMapper) {
    return new EnvelopedAspect()
        .name(envelopedAspect.getName())
        .timestamp(envelopedAspect.getTimestamp())
        .version(envelopedAspect.getVersion())
        .type(AspectType.fromValue(envelopedAspect.getType().name().toUpperCase(Locale.ROOT)))
        .created(objectMapper.convertValue(envelopedAspect.getCreated().data(), AuditStamp.class))
        .value(mapAspectValue(envelopedAspect.getName(), envelopedAspect.getValue(), objectMapper));
  }

  public static OneOfEnvelopedAspectValue mapAspectValue(String aspectName, Aspect aspect, ObjectMapper objectMapper) {
    Class<? extends OneOfEnvelopedAspectValue> aspectClass = ENVELOPED_ASPECT_TYPE_MAP.get(aspectName);
    DataMap wrapper = aspect.data();
    wrapper.put(DISCRIMINATOR, aspectClass.getSimpleName());
    String dataMapAsJson;
    try {
      dataMapAsJson = objectMapper.writeValueAsString(wrapper);
      Matcher classNameMatcher = CLASS_NAME_PATTERN.matcher(dataMapAsJson);
      while (classNameMatcher.find()) {
        String className = classNameMatcher.group("className");
        String content = classNameMatcher.group("content");
        StringBuilder replacement = new StringBuilder("\"" + DISCRIMINATOR + "\" : \"" + className + "\"");

        if (content.length() > 0) {
          replacement.append(",")
              .append(content);
        }
        replacement.append("}");
        dataMapAsJson = classNameMatcher.replaceFirst(Matcher.quoteReplacement(replacement.toString()));
        classNameMatcher = CLASS_NAME_PATTERN.matcher(dataMapAsJson);
      }
      // Global Tags & Glossary Terms will not have the explicit class name in the DataMap, so we handle them differently
      Matcher globalTagsMatcher = GLOBAL_TAGS_PATTERN.matcher(dataMapAsJson);
      while (globalTagsMatcher.find()) {
        String replacement = "\"globalTags\" : {\"" + DISCRIMINATOR + "\" : \"GlobalTags\",";
        dataMapAsJson = globalTagsMatcher.replaceFirst(Matcher.quoteReplacement(replacement));
        globalTagsMatcher = GLOBAL_TAGS_PATTERN.matcher(dataMapAsJson);
      }
      Matcher glossaryTermsMatcher = GLOSSARY_TERMS_PATTERN.matcher(dataMapAsJson);
      while (glossaryTermsMatcher.find()) {
        String replacement = "\"glossaryTerms\" : {\"" + DISCRIMINATOR + "\" : \"GlossaryTerms\",";
        dataMapAsJson = glossaryTermsMatcher.replaceFirst(Matcher.quoteReplacement(replacement));
        glossaryTermsMatcher = GLOSSARY_TERMS_PATTERN.matcher(dataMapAsJson);
      }
      return objectMapper.readValue(dataMapAsJson, aspectClass);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static void putEnvelopedAspectEntry(BeanDefinition beanDefinition) {
    try {
      Class<? extends OneOfEnvelopedAspectValue> cls =
          (Class<? extends OneOfEnvelopedAspectValue>) Class.forName(beanDefinition.getBeanClassName());
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
          (Class<? extends OneOfGenericAspectValue>) Class.forName(beanDefinition.getBeanClassName());
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


  @Nonnull
  public static GenericAspect convertGenericAspect(@Nonnull io.datahubproject.openapi.generated.GenericAspect genericAspect,
      ObjectMapper objectMapper) {
    try {
      ObjectNode jsonTree = (ObjectNode) objectMapper.valueToTree(genericAspect).get("value");
      jsonTree.remove(DISCRIMINATOR);
      String pretty = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonTree);
      Matcher classTypeNameMatcher = CLASS_TYPE_NAME_PATTERN.matcher(pretty);
      while (classTypeNameMatcher.find()) {
        String classTypeName = classTypeNameMatcher.group("classTypeName");
        String content = classTypeNameMatcher.group("content");
        StringBuilder replacement = new StringBuilder();
        // Global Tags & Glossary Terms get used as both a union type and a non-union type, in the DataMap this means
        // that it does not want the explicit class name if it is being used explicitly as a non-union type field on an aspect
        if (!GLOBAL_TAGS.equals(classTypeName) && !GLOSSARY_TERMS.equals(classTypeName)) {
          String pegasusClassName = PEGASUS_TYPE_MAP.get(classTypeName).getName();
          replacement.append("\"").append(pegasusClassName).append("\" : {");

          if (content.length() > 0) {
            replacement.append(content);
          }
          replacement.append("}}");
        } else {
          replacement.append(content)
              .append("}");
        }
        pretty = classTypeNameMatcher.replaceFirst(Matcher.quoteReplacement(replacement.toString()));
        classTypeNameMatcher = CLASS_TYPE_NAME_PATTERN.matcher(pretty);
      }
      return new GenericAspect().setContentType(genericAspect.getContentType())
          .setValue(ByteString.copyString(pretty, UTF_8));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static Pair<String, Boolean> ingestProposal(MetadataChangeProposal metadataChangeProposal, EntityService entityService,
      ObjectMapper objectMapper) {
    // TODO: Use the actor present in the IC.
    Timer.Context context = MetricUtils.timer("postEntity").time();
    final com.linkedin.common.AuditStamp auditStamp =
        new com.linkedin.common.AuditStamp().setTime(System.currentTimeMillis())
            .setActor(UrnUtils.getUrn(Constants.UNKNOWN_ACTOR));
    io.datahubproject.openapi.generated.KafkaAuditHeader auditHeader = metadataChangeProposal.getAuditHeader();

    com.linkedin.mxe.MetadataChangeProposal serviceProposal =
        new com.linkedin.mxe.MetadataChangeProposal()
            .setEntityType(metadataChangeProposal.getEntityType())
            .setChangeType(ChangeType.valueOf(metadataChangeProposal.getChangeType().name()));
    if (metadataChangeProposal.getEntityUrn() != null) {
      serviceProposal.setEntityUrn(UrnUtils.getUrn(metadataChangeProposal.getEntityUrn()));
    }
    if (metadataChangeProposal.getSystemMetadata() != null) {
      serviceProposal.setSystemMetadata(
          objectMapper.convertValue(metadataChangeProposal.getSystemMetadata(), SystemMetadata.class));
    }
    if (metadataChangeProposal.getAspectName() != null) {
      serviceProposal.setAspectName(metadataChangeProposal.getAspectName());
    }

    if (auditHeader != null) {
      KafkaAuditHeader kafkaAuditHeader = new KafkaAuditHeader();
      kafkaAuditHeader.setAuditVersion(auditHeader.getAuditVersion())
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

    serviceProposal = metadataChangeProposal.getEntityKeyAspect() != null
        ? serviceProposal.setEntityKeyAspect(
        MappingUtil.convertGenericAspect(metadataChangeProposal.getEntityKeyAspect(), objectMapper))
        : serviceProposal;
    serviceProposal = metadataChangeProposal.getAspect() != null
        ? serviceProposal.setAspect(
        MappingUtil.convertGenericAspect(metadataChangeProposal.getAspect(), objectMapper))
        : serviceProposal;

    final List<com.linkedin.mxe.MetadataChangeProposal> additionalChanges =
        AspectUtils.getAdditionalChanges(serviceProposal, entityService);

    log.info("Proposal: {}", serviceProposal);
    Throwable exceptionally = null;
    try {
      EntityService.IngestProposalResult proposalResult = entityService.ingestProposal(serviceProposal, auditStamp);
      Urn urn = proposalResult.getUrn();
      additionalChanges.forEach(proposal -> entityService.ingestProposal(proposal, auditStamp));
      return new Pair<>(urn.toString(), proposalResult.isDidUpdate());
    } catch (ValidationException ve) {
      exceptionally = ve;
      throw HttpClientErrorException.create(HttpStatus.UNPROCESSABLE_ENTITY, ve.getMessage(), null, null, null);
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
    MetadataChangeProposal metadataChangeProposal = new MetadataChangeProposal();
    io.datahubproject.openapi.generated.GenericAspect
        genericAspect = new io.datahubproject.openapi.generated.GenericAspect()
        .value(aspectRequest.getAspect())
        .contentType(MediaType.APPLICATION_JSON_VALUE);
    io.datahubproject.openapi.generated.GenericAspect keyAspect = null;
    if (aspectRequest.getEntityKeyAspect() != null) {
      keyAspect = new io.datahubproject.openapi.generated.GenericAspect()
          .contentType(MediaType.APPLICATION_JSON_VALUE)
          .value(aspectRequest.getEntityKeyAspect());
    }
    metadataChangeProposal.aspect(genericAspect)
        .changeType(io.datahubproject.openapi.generated.ChangeType.UPSERT)
        .aspectName(ASPECT_NAME_MAP.get(aspectRequest.getAspect().getClass()))
        .entityKeyAspect(keyAspect)
        .entityUrn(aspectRequest.getEntityUrn())
        .entityType(aspectRequest.getEntityType());

    return metadataChangeProposal;
  }

  public static RollbackRunResultDto mapRollbackRunResult(RollbackRunResult rollbackRunResult, ObjectMapper objectMapper) {
    List<AspectRowSummary> aspectRowSummaries = rollbackRunResult.getRowsRolledBack().stream()
        .map(aspectRowSummary -> objectMapper.convertValue(aspectRowSummary.data(), AspectRowSummary.class))
        .collect(Collectors.toList());
    return RollbackRunResultDto.builder()
        .rowsRolledBack(aspectRowSummaries)
        .rowsDeletedFromEntityDeletion(rollbackRunResult.getRowsDeletedFromEntityDeletion()).build();
  }

  public static UpsertAspectRequest createStatusRemoval(Urn urn, EntityService entityService) {
    EntitySpec entitySpec = entityService.getEntityRegistry().getEntitySpec(urn.getEntityType());
    if (entitySpec == null || !entitySpec.getAspectSpecMap().containsKey(STATUS_ASPECT_NAME)) {
      throw new IllegalArgumentException("Entity type is not valid for soft deletes: " + urn.getEntityType());
    }
    return UpsertAspectRequest.builder()
        .aspect(new Status().removed(true))
        .entityUrn(urn.toString())
        .entityType(urn.getEntityType())
        .build();
  }
}

package io.datahubproject.openapi.util;

import static io.datahubproject.openapi.util.ReflectionCache.toLowerFirst;
import static io.datahubproject.openapi.util.ReflectionCache.toUpperFirst;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.util.Pair;
import io.datahubproject.openapi.dto.UpsertAspectRequest;
import io.datahubproject.openapi.dto.UrnResponseMap;
import io.datahubproject.openapi.generated.EntityResponse;
import io.datahubproject.openapi.generated.OneOfGenericAspectValue;
import io.datahubproject.openapi.generated.SystemMetadata;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OpenApiEntitiesUtil {
  private static final String MODEL_VERSION = "V2";
  private static final String REQUEST_SUFFIX = "Request" + MODEL_VERSION;
  private static final String RESPONSE_SUFFIX = "Response" + MODEL_VERSION;

  private static final String ASPECT_REQUEST_SUFFIX = "Aspect" + REQUEST_SUFFIX;
  private static final String ASPECT_RESPONSE_SUFFIX = "Aspect" + RESPONSE_SUFFIX;
  private static final String ENTITY_REQUEST_SUFFIX = "Entity" + REQUEST_SUFFIX;
  private static final String ENTITY_RESPONSE_SUFFIX = "Entity" + RESPONSE_SUFFIX;

  private OpenApiEntitiesUtil() {}

  private static final ReflectionCache REFLECT =
      ReflectionCache.builder().basePackage("io.datahubproject.openapi.generated").build();

  public static <T> UpsertAspectRequest convertAspectToUpsert(
      String entityUrn, Object aspectRequest, Class<T> aspectRequestClazz) {
    try {
      UpsertAspectRequest.UpsertAspectRequestBuilder builder = UpsertAspectRequest.builder();
      builder.entityType(Urn.createFromString(entityUrn).getEntityType());
      builder.entityUrn(entityUrn);

      // i.e. GlobalTagsAspectRequestV2
      if (aspectRequest != null) {
        // i.e. GlobalTags
        Method valueMethod = REFLECT.lookupMethod(aspectRequestClazz, "getValue");
        Object aspect = valueMethod == null ? null : valueMethod.invoke(aspectRequest);

        if (aspect != null) {
          builder.aspect((OneOfGenericAspectValue) aspect);
          return builder.build();
        }
      }

      return null;
    } catch (Exception e) {
      log.error("Error reflecting urn: {} aspect: {}", entityUrn, aspectRequestClazz.getName());
      throw new RuntimeException(e);
    }
  }

  public static <T> List<UpsertAspectRequest> convertEntityToUpsert(
      Object openapiEntity, Class<T> fromClazz, EntityRegistry entityRegistry) {
    final EntitySpec entitySpec = requestClassToEntitySpec(entityRegistry, fromClazz);

    return entitySpec.getAspectSpecs().stream()
        .map(
            aspectSpec -> {
              try {
                UpsertAspectRequest.UpsertAspectRequestBuilder builder =
                    UpsertAspectRequest.builder();
                builder.entityType(entitySpec.getName());
                builder.entityUrn(
                    (String) REFLECT.lookupMethod(fromClazz, "getUrn").invoke(openapiEntity));

                String upperAspectName = toUpperFirst(aspectSpec.getName());
                Method aspectMethod = REFLECT.lookupMethod(fromClazz, "get" + upperAspectName);

                // i.e. GlobalTagsAspectRequestV2
                Object aspectRequest =
                    aspectMethod == null ? null : aspectMethod.invoke(openapiEntity);
                if (aspectRequest != null) {
                  Class<?> aspectRequestClazz =
                      REFLECT.lookupClass(upperAspectName + ASPECT_REQUEST_SUFFIX);

                  // i.e. GlobalTags
                  Method valueMethod = REFLECT.lookupMethod(aspectRequestClazz, "getValue");
                  Object aspect = valueMethod == null ? null : valueMethod.invoke(aspectRequest);

                  if (aspect != null) {
                    builder.aspect((OneOfGenericAspectValue) aspect);
                    return builder.build();
                  }
                }

                return null;
              } catch (Exception e) {
                log.error(
                    "Error reflecting entity: {} aspect: {}",
                    entitySpec.getName(),
                    aspectSpec.getName());
                throw new RuntimeException(e);
              }
            })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  public static <E, A> Optional<A> convertAspect(
      UrnResponseMap urnResponseMap,
      String aspectName,
      Class<E> entityClazz,
      Class<A> aspectClazz,
      boolean withSystemMetadata) {
    return convertEntity(urnResponseMap, entityClazz, withSystemMetadata)
        .map(
            entity -> {
              try {
                Method aspectMethod =
                    REFLECT.lookupMethod(entityClazz, "get" + toUpperFirst(aspectName));
                return aspectMethod == null ? null : aspectClazz.cast(aspectMethod.invoke(entity));
              } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
              }
            });
  }

  public static <T> Optional<T> convertEntity(
      UrnResponseMap urnResponseMap, Class<T> toClazz, boolean withSystemMetadata) {
    return Optional.ofNullable(urnResponseMap)
        .flatMap(respMap -> respMap.getResponses().entrySet().stream().findFirst())
        .flatMap(
            entry ->
                convertEntities(Set.of(entry), toClazz, withSystemMetadata).stream().findFirst());
  }

  public static <T> List<T> convertEntities(
      Set<Map.Entry<String, EntityResponse>> entityResponseSet,
      Class<T> toClazz,
      boolean withSystemMetadata) {
    if (entityResponseSet != null) {
      return entityResponseSet.stream()
          .map(
              entry -> {
                try {
                  // i.e. DataContractEntityResponseV2.Builder
                  Pair<Class<?>, Object> builderPair = REFLECT.getBuilder(toClazz);
                  Set<String> builderMethods =
                      Arrays.stream(builderPair.getFirst().getMethods())
                          .map(Method::getName)
                          .collect(Collectors.toSet());

                  REFLECT
                      .lookupMethod(builderPair, "urn", String.class)
                      .invoke(builderPair.getSecond(), entry.getKey());

                  entry
                      .getValue()
                      .getAspects()
                      .entrySet()
                      .forEach(
                          aspectEntry -> {
                            try {
                              if (builderMethods.contains(aspectEntry.getKey())) {
                                String upperFirstAspect = toUpperFirst(aspectEntry.getKey());
                                Class<?> aspectClazz = REFLECT.lookupClass(upperFirstAspect);
                                Class<?> aspectRespClazz =
                                    REFLECT.lookupClass(upperFirstAspect + ASPECT_RESPONSE_SUFFIX);
                                Class<?> aspectRespClazzBuilder =
                                    REFLECT.lookupClass(
                                        String.join(
                                            "",
                                            upperFirstAspect,
                                            ASPECT_RESPONSE_SUFFIX,
                                            "$",
                                            upperFirstAspect,
                                            ASPECT_RESPONSE_SUFFIX,
                                            "Builder"));
                                Object aspectBuilder =
                                    REFLECT.lookupMethod(aspectRespClazz, "builder").invoke(null);

                                REFLECT
                                    .lookupMethod(aspectRespClazzBuilder, "value", aspectClazz)
                                    .invoke(aspectBuilder, aspectEntry.getValue().getValue());

                                if (withSystemMetadata) {
                                  REFLECT
                                      .lookupMethod(
                                          aspectRespClazzBuilder,
                                          "systemMetadata",
                                          SystemMetadata.class)
                                      .invoke(
                                          aspectBuilder,
                                          aspectEntry.getValue().getSystemMetadata());
                                }

                                REFLECT
                                    .lookupMethod(
                                        builderPair, aspectEntry.getKey(), aspectRespClazz)
                                    .invoke(
                                        builderPair.getSecond(),
                                        REFLECT
                                            .lookupMethod(aspectRespClazzBuilder, "build")
                                            .invoke(aspectBuilder));
                              }
                            } catch (IllegalAccessException | InvocationTargetException e) {
                              throw new RuntimeException(e);
                            }
                          });

                  return toClazz.cast(
                      REFLECT.lookupMethod(builderPair, "build").invoke(builderPair.getSecond()));
                } catch (IllegalAccessException | InvocationTargetException e) {
                  throw new RuntimeException(e);
                }
              })
          .collect(Collectors.toList());
    }
    return List.of();
  }

  public static <I, T> T convertToResponseAspect(I source, Class<T> targetClazz) {
    if (source != null) {
      try {
        Class<?> sourceClazz = REFLECT.lookupClass(source.getClass().getSimpleName());
        Method valueMethod = REFLECT.lookupMethod(sourceClazz, "getValue");
        Object aspect = valueMethod.invoke(source);

        Pair<Class<?>, Object> builderPair = REFLECT.getBuilder(targetClazz);
        REFLECT
            .lookupMethod(builderPair, "value", valueMethod.getReturnType())
            .invoke(builderPair.getSecond(), aspect);

        return targetClazz.cast(
            REFLECT.lookupMethod(builderPair, "build").invoke(builderPair.getSecond()));
      } catch (InvocationTargetException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  public static <I, T> T convertToResponse(
      I source, Class<T> targetClazz, EntityRegistry entityRegistry) {
    if (source != null) {
      try {
        Class<?> sourceClazz = REFLECT.lookupClass(source.getClass().getSimpleName());
        Pair<Class<?>, Object> builderPair = REFLECT.getBuilder(targetClazz);
        copy(Pair.of(sourceClazz, source), builderPair, "urn");

        final EntitySpec entitySpec = requestClassToEntitySpec(entityRegistry, sourceClazz);
        entitySpec.getAspectSpecs().stream()
            .forEach(
                aspectSpec -> {
                  try {
                    copy(Pair.of(sourceClazz, source), builderPair, aspectSpec.getName());
                  } catch (InvocationTargetException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                  }
                });

        return targetClazz.cast(
            REFLECT.lookupMethod(builderPair, "build").invoke(builderPair.getSecond()));
      } catch (InvocationTargetException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  public static <T, S> Optional<S> convertToScrollResponse(
      Class<S> scrollRespClazz, String scrollId, List<T> entityResults) {
    if (entityResults != null) {
      try {
        Pair<Class<?>, Object> builderPair = REFLECT.getBuilder(scrollRespClazz);
        REFLECT
            .lookupMethod(builderPair.getFirst(), "scrollId", String.class)
            .invoke(builderPair.getSecond(), scrollId);
        REFLECT
            .lookupMethod(builderPair.getFirst(), "entities", List.class)
            .invoke(builderPair.getSecond(), entityResults);

        return Optional.of(
            scrollRespClazz.cast(
                REFLECT.lookupMethod(builderPair, "build").invoke(builderPair.getSecond())));

      } catch (InvocationTargetException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    return Optional.empty();
  }

  private static void copy(
      Pair<Class<?>, Object> sourcePair, Pair<Class<?>, Object> builderPair, String method)
      throws InvocationTargetException, IllegalAccessException {
    Method sourceMethod =
        REFLECT.lookupMethod(sourcePair, String.format("get%s", toUpperFirst(method)));
    if (sourceMethod != null) {
      Class<?> paramClazz = null;
      Object param = null;
      if (sourceMethod.getReturnType().getSimpleName().contains("Request")) {
        Object sourceParam = sourceMethod.invoke(sourcePair.getSecond());
        if (sourceParam != null) {
          paramClazz =
              REFLECT.lookupClass(
                  sourceMethod.getReturnType().getSimpleName().replace("Request", "Response"));
          Pair<Class<?>, Object> aspectBuilder = REFLECT.getBuilder(paramClazz);

          for (Method m : sourceMethod.getReturnType().getMethods()) {
            if (m.getName().startsWith("get") && !Objects.equals("getClass", m.getName())) {
              String getterMethod = m.getName().replaceFirst("^get", "");
              copy(
                  Pair.of(
                      sourceMethod.getReturnType(), sourceMethod.invoke(sourcePair.getSecond())),
                  aspectBuilder,
                  getterMethod);
            }
          }

          param = REFLECT.lookupMethod(aspectBuilder, "build").invoke(aspectBuilder.getSecond());
        }
      } else {
        paramClazz = sourceMethod.getReturnType();
        param = sourceMethod.invoke(sourcePair.getSecond());
      }

      if (param != null) {
        Method targetMethod = REFLECT.lookupMethod(builderPair, toLowerFirst(method), paramClazz);
        targetMethod.invoke(builderPair.getSecond(), param);
      }
    } else {
      log.info(
          "Class {} doesn't container method {}",
          sourcePair.getFirst(),
          String.format("get%s", toUpperFirst(method)));
    }
  }

  public static <T> EntitySpec requestClassToEntitySpec(
      EntityRegistry entityRegistry, Class<T> reqClazz) {
    final String entityType =
        toLowerFirst(reqClazz.getSimpleName().replace(ENTITY_REQUEST_SUFFIX, ""));
    return entityRegistry.getEntitySpec(entityType);
  }

  public static <T> EntitySpec responseClassToEntitySpec(
      EntityRegistry entityRegistry, Class<T> respClazz) {
    String entityType = toLowerFirst(respClazz.getSimpleName().replace(ENTITY_RESPONSE_SUFFIX, ""));
    return entityRegistry.getEntitySpec(entityType);
  }
}

package com.linkedin.metadata.test.query;

import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.template.AbstractArrayTemplate;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.test.config.ValidationResult;
import io.opentelemetry.extension.annotations.WithSpan;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;


@Slf4j
@RequiredArgsConstructor
public class QueryVersionedAspectEvaluator implements BaseQueryEvaluator {
  private final EntityRegistry entityRegistry;
  private final EntityService entityService;
  @Getter
  @Setter
  private QueryEngine queryEngine;

  @Override
  public boolean isEligible(String entityType, TestQuery query) {
    if (query.getQueryParts().isEmpty()) {
      return false;
    }

    EntitySpec entitySpec;
    try {
      entitySpec = entityRegistry.getEntitySpec(entityType);
    } catch (Exception e) {
      log.info("Unknown entity type {} while evaluating {}", entityType, this.getClass().getSimpleName());
      return false;
    }
    return entitySpec.hasAspect(query.getQueryParts().get(0));
  }

  private ValidationResult invalidResultWithMessage(String message) {
    return new ValidationResult(false, Collections.singletonList(message));
  }

  @Override
  public ValidationResult validateQuery(String entityType, TestQuery query) {
    EntitySpec entitySpec;
    try {
      entitySpec = entityRegistry.getEntitySpec(entityType);
    } catch (Exception e) {
      return invalidResultWithMessage(String.format("Unknown entity type %s", entityType));
    }

    String aspect = query.getQueryParts().get(0);
    AspectSpec aspectSpec = entitySpec.getAspectSpec(aspect);
    if (aspectSpec == null) {
      return invalidResultWithMessage(
          String.format("Query %s is invalid for entity type %s: Unknown aspect %s", query, entityType, aspect));
    }

    // Check whether the query matches the schema
    RecordDataSchema schema = aspectSpec.getPegasusSchema();
    for (int i = 1; i < query.getQueryParts().size(); i++) {
      String queryPart = query.getQueryParts().get(i);
      if (!schema.contains(queryPart)) {
        return invalidResultWithMessage(
            String.format("Query %s is invalid for entity type %s: Unknown field %s in record %s", query, entityType,
                queryPart, query.getQueryParts().subList(0, i)));
      }
      RecordDataSchema.Field field = schema.getField(queryPart);
      DataSchema fieldSchema = field.getType();
      // If field is an array get the type of the array element
      while (fieldSchema.getType() == DataSchema.Type.ARRAY) {
        fieldSchema = ((ArrayDataSchema) fieldSchema).getItems();
      }

      // If field is primitive, but there is more query part to traverse, query is invalid
      if (fieldSchema.isPrimitive()) {
        if (i < query.getQueryParts().size() - 1) {
          return invalidResultWithMessage(String.format(
              "Query %s is invalid for entity type %s: Field %s is primitive and thus cannot query further", query,
              entityType, query.getQueryParts().subList(0, i + 1)));
        } else {
          return ValidationResult.validResult();
        }
      } else if (fieldSchema.getType() == DataSchema.Type.RECORD) {
        schema = (RecordDataSchema) fieldSchema;
      } else if (fieldSchema.getType() == DataSchema.Type.TYPEREF) {
        // The field is potentially an urn. check if it is urn
        // If it is of Urn type, return valid
        // TODO validate further when the field is an urn based on the type of the urn
        if (((TyperefDataSchema) fieldSchema).getName().endsWith("Urn")) {
          return ValidationResult.validResult();
        } else {
          return invalidResultWithMessage(String.format(
              "Query %s is invalid for entity type %s: Field %s is typerefed but is not an urn, which is not supported",
              query, entityType, query.getQueryParts().subList(0, i + 1)));
        }
      } else {
        return invalidResultWithMessage(String.format(
            "Query %s is invalid for entity type %s: Field %s is of type union or map, which is not supported", query,
            entityType, query.getQueryParts().subList(0, i + 1)));
      }
    }
    return ValidationResult.validResult();
  }

  @WithSpan
  public Map<Urn, Map<TestQuery, TestQueryResponse>> evaluate(String entityType, Set<Urn> urns,
      Set<TestQuery> queries) {
    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityType);
    Set<String> aspectsToQuery = new HashSet<>();
    for (TestQuery query : queries) {
      String aspect = query.getQueryParts().get(0);
      if (!entitySpec.hasAspect(aspect)) {
        log.error("Unknown aspect {} for entity type {}", aspect, entityType);
        throw new RuntimeException(String.format("Unknown aspect %s for entityType %s", aspect, entityType));
      }
      aspectsToQuery.add(aspect);
    }

    Map<Urn, EntityResponse> batchGetResponse;
    try {
      batchGetResponse = entityService.getEntitiesV2(entityType, urns, aspectsToQuery);
    } catch (URISyntaxException e) {
      log.error("Error while fetching versioned aspects {} for urns {}", aspectsToQuery, urns, e);
      throw new RuntimeException(
          String.format("Error while fetching versioned aspects %s for urns %s", aspectsToQuery, urns));
    }

    Map<String, List<AspectWithUrn>> aspectValuesPerAspect = batchGetResponse.values()
        .stream()
        .flatMap(entityResponse -> deserializeResponse(entityResponse, entitySpec).stream())
        .collect(Collectors.groupingBy(Pair::getKey, Collectors.mapping(Pair::getValue, Collectors.toList())));
    Map<Urn, Map<TestQuery, TestQueryResponse>> finalResult = new HashMap<>();
    for (TestQuery query : queries) {
      Map<Urn, TestQueryResponse> queryResult =
          evaluateQuery(aspectValuesPerAspect.getOrDefault(query.getQueryParts().get(0), Collections.emptyList()),
              query);
      queryResult.forEach((entityUrn, queryResponse) -> {
        if (!finalResult.containsKey(entityUrn)) {
          finalResult.put(entityUrn, new HashMap<>());
        }
        finalResult.get(entityUrn).put(query, queryResponse);
      });
    }
    return finalResult;
  }

  private Map<Urn, TestQueryResponse> evaluateQuery(List<AspectWithUrn> aspects, TestQuery query) {
    List<ValueWithUrn> currentValues = aspects.stream()
        .map(aspect -> new ValueWithUrn(aspect.getUrn(), aspect.getAspect()))
        .collect(Collectors.toList());
    for (int i = 1; i < query.getQueryParts().size(); i++) {
      String queryPart = query.getQueryParts().get(i);
      PathSpec pathSpec = new PathSpec(queryPart);
      // If current values is empty, there is no point traversing further
      if (currentValues.isEmpty()) {
        return Collections.emptyMap();
      }
      // If the traversed object is a record template, fetch the field corresponding to the current query part
      if (currentValues.get(0).getValue() instanceof RecordTemplate) {
        List<ValueWithUrn> flatMappedResult = new ArrayList<>();
        for (ValueWithUrn currentValue : currentValues) {
          Optional<Object> fieldValue = RecordUtils.getFieldValue(currentValue.getValue(), pathSpec);
          if (!fieldValue.isPresent()) {
            continue;
          }
          if (fieldValue.get() instanceof AbstractArrayTemplate) {
            AbstractArrayTemplate<Object> arrayFieldValues = (AbstractArrayTemplate<Object>) fieldValue.get();
            arrayFieldValues.forEach(value -> flatMappedResult.add(new ValueWithUrn(currentValue.getUrn(), value)));
          } else {
            flatMappedResult.add(new ValueWithUrn(currentValue.getUrn(), fieldValue.get()));
          }
        }
        currentValues = flatMappedResult;
        // If the traversed object is an urn, recursively evaluate the rest of the query using the query engine
      } else if (currentValues.get(0).getValue() instanceof Urn) {
        TestQuery partialQuery = new TestQuery(query.getQueryParts().subList(i, query.getQueryParts().size()));
        Map<Urn, List<Urn>> valueUrnToSourceUrn = currentValues.stream()
            .collect(Collectors.groupingBy(valueWithUrn -> (Urn) valueWithUrn.getValue(),
                Collectors.mapping(ValueWithUrn::getUrn, Collectors.toList())));
        Map<Urn, Map<TestQuery, TestQueryResponse>> evaluatedPartialQuery =
            queryEngine.batchEvaluate(valueUrnToSourceUrn.keySet(), Collections.singleton(partialQuery));
        Map<Urn, TestQueryResponse> finalResult = new HashMap<>();
        for (Urn valueUrn : evaluatedPartialQuery.keySet()) {
          if (!evaluatedPartialQuery.get(valueUrn).containsKey(partialQuery)) {
            continue;
          }
          TestQueryResponse partialQueryResponse = evaluatedPartialQuery.get(valueUrn).get(partialQuery);
          if (partialQueryResponse.getValues().isEmpty()) {
            continue;
          }

          List<Urn> sourceUrns = valueUrnToSourceUrn.get(valueUrn);
          for (Urn sourceUrn : sourceUrns) {
            if (!finalResult.containsKey(sourceUrn)) {
              finalResult.put(sourceUrn, new TestQueryResponse(new ArrayList<>()));
            }
            finalResult.get(sourceUrn).getValues().addAll(partialQueryResponse.getValues());
          }
        }
        return finalResult;
      } else {
        log.error("Invalid metadata test query: cannot fetch field {} of objects {}", queryPart, currentValues);
        throw new UnsupportedOperationException(
            String.format("Invalid metadata test query: cannot fetch field %s of objects %s", queryPart,
                currentValues));
      }
    }
    return currentValues.stream()
        .collect(Collectors.groupingBy(ValueWithUrn::getUrn, Collectors.collectingAndThen(
            Collectors.mapping(valueWithUrn -> valueWithUrn.getValue().toString(), Collectors.toList()),
            TestQueryResponse::new)));
  }

  private static List<Pair<String, AspectWithUrn>> deserializeResponse(EntityResponse entityResponse,
      EntitySpec entitySpec) {
    return entityResponse.getAspects()
        .entrySet()
        .stream()
        .map(entry -> Pair.of(entry.getKey(), new AspectWithUrn(entityResponse.getUrn(),
            deserializeEnvelopedAspect(entry.getValue().getValue(), entitySpec.getAspectSpec(entry.getKey())))))
        .collect(Collectors.toList());
  }

  private static RecordTemplate deserializeEnvelopedAspect(Aspect aspect, AspectSpec aspectSpec) {
    return RecordUtils.toRecordTemplate(aspectSpec.getDataTemplateClass(), aspect.data());
  }

  @Value
  private static class AspectWithUrn {
    Urn urn;
    RecordTemplate aspect;
  }

  @Value
  private static class ValueWithUrn {
    Urn urn;
    Object value;
  }
}

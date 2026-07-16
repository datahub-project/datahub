package com.linkedin.metadata.graph.cache.client;

import static com.linkedin.metadata.Constants.CHART_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CONTAINER_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CONTAINER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DASHBOARD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_FLOW_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_JOB_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOMAIN_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOMAIN_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_NODE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_NODE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERM_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERM_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.IS_PART_OF_RELATIONSHIP_NAME;
import static com.linkedin.metadata.Constants.ML_MODEL_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ML_MODEL_GROUP_ENTITY_NAME;

import com.linkedin.container.Container;
import com.linkedin.domain.DomainProperties;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.ResolvedGraphEdge;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.experimental.UtilityClass;

/** Built-in {@link HierarchyReadSpec} factories for first-party entity graphs. */
@UtilityClass
public class HierarchyReadSpecs {

  private static final String ML_MODEL_DEPLOYMENT_ENTITY_NAME = "mlModelDeployment";

  private static final List<String> CONTAINER_ASPECT_ENTITY_TYPES =
      List.of(
          CONTAINER_ENTITY_NAME,
          DATASET_ENTITY_NAME,
          CHART_ENTITY_NAME,
          DASHBOARD_ENTITY_NAME,
          DATA_FLOW_ENTITY_NAME,
          DATA_JOB_ENTITY_NAME,
          DATA_PROCESS_INSTANCE_ENTITY_NAME,
          ML_MODEL_ENTITY_NAME,
          ML_MODEL_GROUP_ENTITY_NAME,
          ML_MODEL_DEPLOYMENT_ENTITY_NAME);

  private static final ParentAspectSpec DOMAIN_PARENT_ASPECT =
      ParentAspectSpec.builder()
          .entityType(DOMAIN_ENTITY_NAME)
          .aspectName(DOMAIN_PROPERTIES_ASPECT_NAME)
          .parentExtractor(
              data -> {
                DomainProperties properties = new DomainProperties(data);
                return properties.hasParentDomain()
                    ? Optional.of(properties.getParentDomain())
                    : Optional.empty();
              })
          .build();

  private static final ParentAspectSpec GLOSSARY_NODE_PARENT_ASPECT =
      ParentAspectSpec.builder()
          .entityType(GLOSSARY_NODE_ENTITY_NAME)
          .aspectName(GLOSSARY_NODE_INFO_ASPECT_NAME)
          .parentExtractor(
              data -> {
                GlossaryNodeInfo nodeInfo = new GlossaryNodeInfo(data);
                return nodeInfo.hasParentNode()
                    ? Optional.of(nodeInfo.getParentNode())
                    : Optional.empty();
              })
          .build();

  private static final ParentAspectSpec GLOSSARY_TERM_PARENT_ASPECT =
      ParentAspectSpec.builder()
          .entityType(GLOSSARY_TERM_ENTITY_NAME)
          .aspectName(GLOSSARY_TERM_INFO_ASPECT_NAME)
          .parentExtractor(
              data -> {
                GlossaryTermInfo termInfo = new GlossaryTermInfo(data);
                return termInfo.hasParentNode()
                    ? Optional.of(termInfo.getParentNode())
                    : Optional.empty();
              })
          .build();

  @Nonnull
  public static Optional<HierarchyReadSpec> forKnownGraph(
      @Nonnull KnownEntityGraph known, @Nonnull EntityGraphBinding binding) {
    return switch (known) {
      case DOMAIN -> Optional.of(domain(binding));
      case GLOSSARY -> Optional.of(glossary(binding));
      case CONTAINER -> Optional.of(container(binding));
      case MEMBERSHIP -> Optional.empty();
    };
  }

  @Nonnull
  public static Optional<HierarchyReadSpec> forKnownGraph(
      @Nonnull KnownEntityGraph known,
      @Nonnull EntityGraphBinding binding,
      @Nonnull EntityGraphDefinition definition) {
    return forKnownGraph(known, binding)
        .map(
            spec ->
                HierarchyReadSpec.builder()
                    .binding(spec.getBinding())
                    .parentAspectsByEntityType(spec.getParentAspectsByEntityType())
                    .scrollSourceEntityTypes(scrollSourceEntityTypes(definition))
                    .scrollDestinationEntityTypes(scrollDestinationEntityTypes(definition))
                    .relationshipType(spec.getRelationshipType())
                    .build());
  }

  @Nonnull
  public static HierarchyReadSpec domain(@Nonnull EntityGraphBinding binding) {
    return HierarchyReadSpec.builder()
        .binding(binding)
        .parentAspectsByEntityType(Map.of(DOMAIN_ENTITY_NAME, DOMAIN_PARENT_ASPECT))
        .scrollSourceEntityTypes(Set.of(DOMAIN_ENTITY_NAME))
        .scrollDestinationEntityTypes(Set.of(DOMAIN_ENTITY_NAME))
        .relationshipType(IS_PART_OF_RELATIONSHIP_NAME)
        .build();
  }

  @Nonnull
  public static HierarchyReadSpec glossary(@Nonnull EntityGraphBinding binding) {
    return HierarchyReadSpec.builder()
        .binding(binding)
        .parentAspectsByEntityType(
            Map.of(
                GLOSSARY_NODE_ENTITY_NAME,
                GLOSSARY_NODE_PARENT_ASPECT,
                GLOSSARY_TERM_ENTITY_NAME,
                GLOSSARY_TERM_PARENT_ASPECT))
        .scrollSourceEntityTypes(Set.of(GLOSSARY_NODE_ENTITY_NAME, GLOSSARY_TERM_ENTITY_NAME))
        .scrollDestinationEntityTypes(Set.of(GLOSSARY_NODE_ENTITY_NAME))
        .relationshipType(IS_PART_OF_RELATIONSHIP_NAME)
        .build();
  }

  @Nonnull
  public static HierarchyReadSpec container(@Nonnull EntityGraphBinding binding) {
    Map<String, ParentAspectSpec> parentAspectsByEntityType = new LinkedHashMap<>();
    for (String entityType : CONTAINER_ASPECT_ENTITY_TYPES) {
      parentAspectsByEntityType.put(entityType, containerParentAspectFor(entityType));
    }
    return HierarchyReadSpec.builder()
        .binding(binding)
        .parentAspectsByEntityType(parentAspectsByEntityType)
        .scrollSourceEntityTypes(Set.of(CONTAINER_ENTITY_NAME))
        .scrollDestinationEntityTypes(Set.of(CONTAINER_ENTITY_NAME))
        .relationshipType(IS_PART_OF_RELATIONSHIP_NAME)
        .build();
  }

  @Nonnull
  private static ParentAspectSpec containerParentAspectFor(@Nonnull String entityType) {
    return ParentAspectSpec.builder()
        .entityType(entityType)
        .aspectName(CONTAINER_ASPECT_NAME)
        .parentExtractor(
            data -> {
              Container container = new Container(data);
              return container.hasContainer()
                  ? Optional.of(container.getContainer())
                  : Optional.empty();
            })
        .build();
  }

  @Nonnull
  private static Set<String> scrollSourceEntityTypes(@Nonnull EntityGraphDefinition definition) {
    return definition.getResolvedEdges().stream()
        .map(ResolvedGraphEdge::getTriplet)
        .map(triplet -> triplet.getSourceEntityType())
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  @Nonnull
  private static Set<String> scrollDestinationEntityTypes(
      @Nonnull EntityGraphDefinition definition) {
    return definition.getResolvedEdges().stream()
        .map(ResolvedGraphEdge::getTriplet)
        .map(triplet -> triplet.getDestinationEntityType())
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }
}

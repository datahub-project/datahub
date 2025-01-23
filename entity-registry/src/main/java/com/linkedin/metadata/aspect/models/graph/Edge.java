package com.linkedin.metadata.aspect.models.graph;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.util.Pair;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Data
@AllArgsConstructor
@Slf4j
public class Edge {
  @EqualsAndHashCode.Include private Urn source;
  @EqualsAndHashCode.Include private Urn destination;
  @EqualsAndHashCode.Include private String relationshipType;
  @EqualsAndHashCode.Exclude private Long createdOn;
  @EqualsAndHashCode.Exclude private Urn createdActor;
  @EqualsAndHashCode.Exclude private Long updatedOn;
  @EqualsAndHashCode.Exclude private Urn updatedActor;
  @EqualsAndHashCode.Exclude private Map<String, Object> properties;
  // The entity who owns the lifecycle of this edge
  @EqualsAndHashCode.Include private Urn lifecycleOwner;
  // An entity through which the edge between source and destination is created
  @EqualsAndHashCode.Include private Urn via;
  @EqualsAndHashCode.Exclude @Nullable private Boolean sourceStatus;
  @EqualsAndHashCode.Exclude @Nullable private Boolean destinationStatus;
  @EqualsAndHashCode.Exclude @Nullable private Boolean viaStatus;
  @EqualsAndHashCode.Exclude @Nullable private Boolean lifecycleOwnerStatus;

  // For backwards compatibility
  public Edge(
      Urn source,
      Urn destination,
      String relationshipType,
      Long createdOn,
      Urn createdActor,
      Long updatedOn,
      Urn updatedActor,
      Map<String, Object> properties) {
    this(
        source,
        destination,
        relationshipType,
        createdOn,
        createdActor,
        updatedOn,
        updatedActor,
        properties,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  public Edge(
      Urn source,
      Urn destination,
      String relationshipType,
      Long createdOn,
      Urn createdActor,
      Long updatedOn,
      Urn updatedActor,
      Map<String, Object> properties,
      Urn lifecycleOwner,
      Urn via) {
    this(
        source,
        destination,
        relationshipType,
        createdOn,
        createdActor,
        updatedOn,
        updatedActor,
        properties,
        lifecycleOwner,
        via,
        null,
        null,
        null,
        null);
  }

  public String toDocId(@Nonnull String idHashAlgo) {
    StringBuilder rawDocId = new StringBuilder();
    rawDocId
        .append(getSource().toString())
        .append(DOC_DELIMETER)
        .append(getRelationshipType())
        .append(DOC_DELIMETER)
        .append(getDestination().toString());
    if (getLifecycleOwner() != null && StringUtils.isNotBlank(getLifecycleOwner().toString())) {
      rawDocId.append(DOC_DELIMETER).append(getLifecycleOwner().toString());
    }

    try {
      byte[] bytesOfRawDocID = rawDocId.toString().getBytes(StandardCharsets.UTF_8);
      MessageDigest md = MessageDigest.getInstance(idHashAlgo);
      byte[] thedigest = md.digest(bytesOfRawDocID);
      return Base64.getEncoder().encodeToString(thedigest);
    } catch (NoSuchAlgorithmException e) {
      log.error("Unable to hash document ID, returning unhashed id: " + rawDocId);
      return rawDocId.toString();
    }
  }

  public static final String EDGE_FIELD_SOURCE = "source";
  public static final String EDGE_FIELD_DESTINATION = "destination";
  public static final String EDGE_FIELD_RELNSHIP_TYPE = "relationshipType";
  public static final String EDGE_FIELD_PROPERTIES = "properties";
  public static final String EDGE_FIELD_VIA = "via";
  public static final String EDGE_FIELD_LIFECYCLE_OWNER = "lifecycleOwner";
  public static final String EDGE_SOURCE_URN_FIELD = "source.urn";
  public static final String EDGE_DESTINATION_URN_FIELD = "destination.urn";
  public static final String EDGE_SOURCE_STATUS = "source.removed";
  public static final String EDGE_DESTINATION_STATUS = "destination.removed";
  public static final String EDGE_FIELD_VIA_STATUS = "viaRemoved";
  public static final String EDGE_FIELD_LIFECYCLE_OWNER_STATUS = "lifecycleOwnerRemoved";

  public static final List<Pair<String, SortOrder>> KEY_SORTS =
      ImmutableList.of(
          new Pair<>(EDGE_SOURCE_URN_FIELD, SortOrder.ASCENDING),
          new Pair<>(EDGE_DESTINATION_URN_FIELD, SortOrder.ASCENDING),
          new Pair<>(EDGE_FIELD_RELNSHIP_TYPE, SortOrder.ASCENDING),
          new Pair<>(EDGE_FIELD_LIFECYCLE_OWNER, SortOrder.ASCENDING));
  public static List<SortCriterion> EDGE_SORT_CRITERION =
      KEY_SORTS.stream()
          .map(
              entry -> {
                SortCriterion sortCriterion = new SortCriterion();
                sortCriterion.setField(entry.getKey());
                sortCriterion.setOrder(
                    com.linkedin.metadata.query.filter.SortOrder.valueOf(
                        Optional.ofNullable(entry.getValue())
                            .orElse(SortOrder.ASCENDING)
                            .toString()));
                return sortCriterion;
              })
          .collect(Collectors.toList());
  private static final String DOC_DELIMETER = "--";
}

package io.datahubproject.openlineage.converter;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Edge;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.TupleKey;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.domain.Domains;
import com.linkedin.mxe.MetadataChangeProposal;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class OpenLineageMappingUtils {

  static final String URN_LI_CORPUSER = "urn:li:corpuser:";
  static final String URN_LI_CORPUSER_DATAHUB = URN_LI_CORPUSER + "datahub";

  private OpenLineageMappingUtils() {}

  static GlobalTags generateTags(List<String> tags) {
    tags.sort(String::compareToIgnoreCase);
    GlobalTags globalTags = new GlobalTags();
    TagAssociationArray tagAssociationArray = new TagAssociationArray();
    for (String tag : tags) {
      TagAssociation tagAssociation = new TagAssociation();
      tagAssociation.setTag(new TagUrn(tag));
      tagAssociationArray.add(tagAssociation);
    }
    globalTags.setTags(tagAssociationArray);
    return globalTags;
  }

  static <T> GlobalTags generateFacetTags(
      List<T> tagFacets, Function<T, String> keySelector, Function<T, String> valueSelector) {
    if (tagFacets == null) {
      return null;
    }
    LinkedList<String> tags = new LinkedList<>();
    for (T tagFacet : tagFacets) {
      String key = keySelector.apply(tagFacet);
      String tagName = key != null ? key : valueSelector.apply(tagFacet);
      if (tagName != null && !tagName.isEmpty()) {
        tags.add(tagName);
      }
    }
    return tags.isEmpty() ? null : generateTags(tags);
  }

  static <T> Ownership generateOwnership(
      List<T> ownerFacets, Function<T, String> nameSelector, Function<T, String> typeSelector) {
    if (ownerFacets == null) {
      return null;
    }
    OwnerArray owners = new OwnerArray();
    for (T ownerFacet : ownerFacets) {
      String ownerName = nameSelector.apply(ownerFacet);
      try {
        owners.add(
            new Owner()
                .setOwner(Urn.createFromString(URN_LI_CORPUSER + ownerName))
                .setType(mapOwnershipType(typeSelector.apply(ownerFacet)))
                .setSource(new OwnershipSource().setType(OwnershipSourceType.SERVICE)));
      } catch (URISyntaxException exception) {
        log.warn("Unable to create owner urn for owner: {}", ownerName);
      }
    }
    return owners.isEmpty()
        ? null
        : new Ownership().setOwners(owners).setLastModified(createAuditStamp(null));
  }

  static Domains generateDomains(List<String> domains) {
    domains.sort(String::compareToIgnoreCase);
    Domains datahubDomains = new Domains();
    UrnArray domainArray = new UrnArray();
    for (String domain : domains) {
      try {
        domainArray.add(Urn.createFromString(domain));
      } catch (URISyntaxException e) {
        log.warn("Unable to create domain urn for domain urn: {}", domain);
      }
    }
    datahubDomains.setDomains(domainArray);
    return datahubDomains;
  }

  static Urn dataPlatformInstanceUrn(String platform, String instance) {
    return new Urn(
        "dataPlatformInstance",
        new TupleKey(Arrays.asList(new DataPlatformUrn(platform).toString(), instance)));
  }

  static OwnershipType mapOwnershipType(String openLineageOwnershipType) {
    if (openLineageOwnershipType == null || openLineageOwnershipType.isEmpty()) {
      return OwnershipType.TECHNICAL_OWNER;
    }
    try {
      return OwnershipType.valueOf(openLineageOwnershipType.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      return OwnershipType.TECHNICAL_OWNER;
    }
  }

  static boolean putIfPresent(StringMap customProperties, String key, String value) {
    if (value == null || value.isBlank()) {
      return false;
    }
    customProperties.put(key, value);
    return true;
  }

  static void addAspectToMcps(
      Urn entityUrn,
      String entityType,
      com.linkedin.data.template.DataTemplate aspect,
      List<MetadataChangeProposal> mcps) {
    mcps.add(OpenLineageMcpFactory.upsert(entityUrn, entityType, aspect));
  }

  static void logFacetNames(String eventType, String attachmentPoint, Set<String> names) {
    if (names == null) {
      return;
    }
    for (String name : names) {
      log.debug(
          "Skipping unmapped OpenLineage facet '{}' on {} attachment point {}",
          name,
          eventType,
          attachmentPoint);
    }
  }

  static Edge createEdge(Urn urn, ZonedDateTime eventTime) {
    Edge edge = new Edge();
    edge.setLastModified(createAuditStamp(eventTime));
    edge.setDestinationUrn(urn);
    return edge;
  }

  static AuditStamp createAuditStamp(ZonedDateTime eventTime) {
    AuditStamp auditStamp = new AuditStamp();
    if (eventTime != null) {
      auditStamp.setTime(eventTime.toInstant().toEpochMilli());
    } else {
      auditStamp.setTime(System.currentTimeMillis());
    }
    try {
      auditStamp.setActor(Urn.createFromString(URN_LI_CORPUSER_DATAHUB));
    } catch (URISyntaxException e) {
      throw new RuntimeException("Unable to create actor urn:" + e);
    }
    return auditStamp;
  }
}

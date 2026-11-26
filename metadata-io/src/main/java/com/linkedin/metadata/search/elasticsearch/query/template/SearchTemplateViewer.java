package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.CORP_GROUP_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DOMAIN_ENTITY_NAME;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.graph.cache.client.BoundHierarchyAccess;
import com.linkedin.metadata.graph.cache.client.HierarchyBindings;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import com.linkedin.policy.PolicyMatchCondition;
import com.linkedin.policy.PolicyMatchCriterion;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Injectable caller values for functionScore and clauses. Authorization stays outside the template.
 */
public final class SearchTemplateViewer {
  public static final String URN = "viewerUrn";
  public static final String GROUPS = "viewerGroupsJson";
  public static final String DOMAINS = "viewerDomainsJson";
  public static final String DOMAIN_LIST = "domainsJson";
  public static final Set<String> INJECTABLES = Set.of(URN, GROUPS, DOMAINS, DOMAIN_LIST);

  private static final String OWNED_BY = "OwnedBy";
  private static final String DOMAIN_FIELD = "DOMAIN";

  private SearchTemplateViewer() {}

  public static boolean references(@Nonnull String blob) {
    return blob.contains(URN)
        || blob.contains(GROUPS)
        || blob.contains(DOMAINS)
        || blob.contains(DOMAIN_LIST);
  }

  @Nonnull
  public static String urn(@Nonnull OperationContext opContext) {
    try {
      Urn actor = opContext.getSessionActorContext().getActorUrn();
      return actor == null ? "" : actor.toString();
    } catch (RuntimeException e) {
      return "";
    }
  }

  @Nonnull
  public static String groupsJson(@Nonnull OperationContext opContext) {
    try {
      Collection<Urn> groups = opContext.getActorGroups();
      return json(groups);
    } catch (RuntimeException e) {
      return "[]";
    }
  }

  @Nonnull
  public static String domainsJson(@Nonnull OperationContext opContext) {
    try {
      Urn actor = opContext.getSessionActorContext().getActorUrn();
      if (actor == null || opContext.getAspectRetriever() == null) {
        return "[]";
      }
      Aspect aspect =
          opContext
              .getAspectRetriever()
              .getLatestAspectObject(opContext, actor, DOMAINS_ASPECT_NAME);
      if (aspect == null || aspect.data() == null) {
        return "[]";
      }
      Domains domains = new Domains(aspect.data());
      if (domains.getDomains() == null) {
        return "[]";
      }
      return json(domains.getDomains());
    } catch (RuntimeException e) {
      return "[]";
    }
  }

  /**
   * Domains the caller owns, domains named on an active policy that grants the caller a privilege
   * on that domain, and the descendants of either. A platform-wide policy leaves the list empty.
   */
  @Nonnull
  public static String domainListJson(@Nonnull OperationContext opContext) {
    try {
      Urn actor = opContext.getSessionActorContext().getActorUrn();
      if (actor == null) {
        return "[]";
      }
      Set<Urn> owners = new LinkedHashSet<>();
      owners.add(actor);
      Collection<Urn> groups = opContext.getActorGroups();
      if (groups != null) {
        owners.addAll(groups);
      }
      Set<Urn> seeds = new LinkedHashSet<>(ownedDomains(opContext, owners));
      seeds.addAll(
          domainUrnsFromPolicies(
              opContext.getAuthorizationContext().getAuthorizer().getActorPolicies(actor)));
      return json(includeSeeds(seeds, seed -> descendants(opContext, seed)));
    } catch (RuntimeException e) {
      return "[]";
    }
  }

  @Nonnull
  public static String identity(@Nonnull OperationContext opContext, @Nonnull String blob) {
    StringBuilder identity = new StringBuilder();
    if (blob.contains(URN)) {
      identity.append("@viewer=").append(urn(opContext));
    }
    if (blob.contains(GROUPS)) {
      identity.append("@groups=").append(groupsJson(opContext));
    }
    if (blob.contains(DOMAINS)) {
      identity.append("@domains=").append(domainsJson(opContext));
    }
    if (blob.contains(DOMAIN_LIST)) {
      identity.append("@domains=").append(domainListJson(opContext));
    }
    return identity.toString();
  }

  @Nonnull
  public static List<String> list(@Nonnull OperationContext opContext, @Nonnull String name) {
    if (!INJECTABLES.contains(name)) {
      throw new SearchTemplateException("Search template boost values are not valid: " + name);
    }
    if (URN.equals(name)) {
      String urn = urn(opContext);
      return urn.isBlank() ? List.of() : List.of(urn);
    }
    String json =
        GROUPS.equals(name)
            ? groupsJson(opContext)
            : DOMAINS.equals(name) ? domainsJson(opContext) : domainListJson(opContext);
    try {
      JsonNode node = SearchTemplateMappers.JSON.readTree(json);
      if (!node.isArray()) {
        return List.of();
      }
      List<String> values = new ArrayList<>();
      for (JsonNode value : node) {
        if (value.isTextual() && !value.asText().isBlank()) {
          values.add(value.asText());
        }
      }
      return values;
    } catch (Exception e) {
      return List.of();
    }
  }

  @Nonnull
  static Set<Urn> domainUrnsFromPolicies(@Nullable Collection<DataHubPolicyInfo> policies) {
    Set<Urn> domains = new LinkedHashSet<>();
    if (policies == null) {
      return domains;
    }
    for (DataHubPolicyInfo policy : policies) {
      if (policy == null || !PoliciesConfig.ACTIVE_POLICY_STATE.equals(policy.getState())) {
        continue;
      }
      if (!policy.hasResources() || policy.getResources() == null) {
        continue;
      }
      DataHubResourceFilter resources = policy.getResources();
      if (!resources.hasFilter()
          || resources.getFilter() == null
          || resources.getFilter().getCriteria() == null) {
        continue;
      }
      for (PolicyMatchCriterion criterion : resources.getFilter().getCriteria()) {
        if (criterion == null || !DOMAIN_FIELD.equals(criterion.getField())) {
          continue;
        }
        PolicyMatchCondition condition = criterion.getCondition();
        if (condition != null && condition != PolicyMatchCondition.EQUALS) {
          continue;
        }
        if (criterion.getValues() == null) {
          continue;
        }
        for (String value : criterion.getValues()) {
          Urn urn = domainUrn(value);
          if (urn != null) {
            domains.add(urn);
          }
        }
      }
    }
    return domains;
  }

  /**
   * {@code descendantsOf} returns children only. Seeds stay in the set so a parent domain still
   * matches assets tagged with that domain.
   */
  @Nonnull
  static Set<Urn> includeSeeds(
      @Nonnull Collection<Urn> seeds, @Nonnull Function<Urn, Collection<Urn>> descendantsOf) {
    Set<Urn> all = new LinkedHashSet<>();
    for (Urn seed : seeds) {
      if (seed == null) {
        continue;
      }
      all.add(seed);
      Collection<Urn> children = descendantsOf.apply(seed);
      if (children == null) {
        continue;
      }
      for (Urn child : children) {
        if (child != null) {
          all.add(child);
        }
      }
    }
    return all;
  }

  @Nonnull
  private static Collection<Urn> descendants(
      @Nonnull OperationContext opContext, @Nonnull Urn seed) {
    return BoundHierarchyAccess.expandDescendants(
        opContext, HierarchyBindings.domainSpec(opContext), seed, Integer.MAX_VALUE);
  }

  @Nonnull
  private static Set<Urn> ownedDomains(
      @Nonnull OperationContext opContext, @Nonnull Collection<Urn> owners) {
    List<Urn> present = new ArrayList<>();
    for (Urn owner : owners) {
      if (owner != null) {
        present.add(owner);
      }
    }
    if (present.isEmpty() || opContext.getRetrieverContext() == null) {
      return Set.of();
    }
    GraphRetriever graph = opContext.getRetrieverContext().getGraphRetriever();
    if (graph == null || graph == GraphRetriever.EMPTY) {
      return Set.of();
    }
    List<String> values = new ArrayList<>();
    for (Urn owner : present) {
      values.add(owner.toString());
    }
    Set<Urn> domains = new LinkedHashSet<>();
    String scrollId = null;
    while (true) {
      RelatedEntitiesScrollResult result =
          graph.scrollRelatedEntities(
              Set.of(DOMAIN_ENTITY_NAME),
              QueryUtils.EMPTY_FILTER,
              Set.of(CORP_USER_ENTITY_NAME, CORP_GROUP_ENTITY_NAME),
              QueryUtils.newDisjunctiveFilter(
                  CriterionUtils.buildCriterion("urn", Condition.EQUAL, values)),
              Set.of(OWNED_BY),
              QueryUtils.newRelationshipFilter(
                  QueryUtils.EMPTY_FILTER, RelationshipDirection.OUTGOING),
              Edge.EDGE_SORT_CRITERION,
              scrollId,
              GraphRetriever.DEFAULT_EDGE_FETCH_LIMIT,
              null,
              null);
      if (result.getEntities() != null) {
        for (RelatedEntities related : result.getEntities()) {
          Urn source = domainUrn(related.getSourceUrn());
          if (source != null) {
            domains.add(source);
          }
        }
      }
      if (result.getScrollId() == null || result.getScrollId().equals(scrollId)) {
        break;
      }
      scrollId = result.getScrollId();
    }
    return domains;
  }

  @Nullable
  private static Urn domainUrn(@Nullable String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      Urn urn = UrnUtils.getUrn(value);
      return DOMAIN_ENTITY_NAME.equals(urn.getEntityType()) ? urn : null;
    } catch (RuntimeException e) {
      return null;
    }
  }

  @Nonnull
  private static String json(@Nonnull Collection<Urn> urns) {
    List<String> sorted = new ArrayList<>();
    for (Urn urn : urns) {
      if (urn != null) {
        sorted.add(urn.toString());
      }
    }
    sorted.sort(Comparator.naturalOrder());
    StringBuilder out = new StringBuilder("[");
    for (int i = 0; i < sorted.size(); i++) {
      if (i > 0) {
        out.append(',');
      }
      out.append('"').append(SearchTemplateMustache.jsonEscape(sorted.get(i))).append('"');
    }
    out.append(']');
    return out.toString();
  }
}

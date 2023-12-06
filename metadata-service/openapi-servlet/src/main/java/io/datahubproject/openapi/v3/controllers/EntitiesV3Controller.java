package io.datahubproject.openapi.v3.controllers;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.utils.SearchUtil;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.v3.models.GenericEntity;
import io.datahubproject.openapi.v3.models.GenericScrollResult;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController
@RequiredArgsConstructor
@RequestMapping("/v3/entity")
@Slf4j
@Tag(name = "Entities", description = "APIs for ingesting and accessing entities and their constituent aspects")
public class EntitiesV3Controller {
    final private static SearchFlags DEFAULT_SEARCH_FLAGS = new SearchFlags()
            .setFulltext(false)
            .setSkipAggregates(true)
            .setSkipHighlighting(true);
    @Autowired
    private EntityRegistry entityRegistry;
    @Autowired
    private SearchService searchService;
    @Autowired
    private EntityService entityService;
    @Autowired
    private AuthorizerChain authorizationChain;
    @Autowired
    private boolean restApiAuthorizationEnabled;
    @Autowired
    private ObjectMapper objectMapper;

    @GetMapping(value = "/{entityName}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<GenericScrollResult> getEntities(@PathVariable("entityName") String entityName,
                                                           @RequestParam(value = "aspectNames", defaultValue = "") Set<String> aspectNames,
                                                           @RequestParam(value = "count", defaultValue = "10") Integer count,
                                                           @RequestParam(value = "query", defaultValue = "*") String query,
                                                           @RequestParam(value = "scrollId", required = false) String scrollId) {
        Authentication authentication = AuthenticationContext.getAuthentication();
        EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
        checkAuthorized(authentication, entitySpec);

        // TODO: sort params
        SortCriterion sortCriterion = SearchUtil.sortBy("urn", SortOrder.ASCENDING);

        ScrollResult result = searchService.scrollAcrossEntities(
                List.of(entitySpec.getName()), query, null,
                sortCriterion, scrollId, null, count, DEFAULT_SEARCH_FLAGS);

        return ResponseEntity.ok(GenericScrollResult.builder()
                .entities(toRecordTemplates(result.getEntities(), aspectNames))
                .scrollId(result.getScrollId()).build());
    }

    @GetMapping(value = "/{entityName}/{entityUrn}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<GenericEntity> getEntity(@PathVariable("entityName") String entityName,
                                            @PathVariable("entityUrn") String entityUrn,
                                            @RequestParam(value = "aspectNames", defaultValue = "") Set<String> aspectNames) {
        Authentication authentication = AuthenticationContext.getAuthentication();
        EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
        checkAuthorized(authentication, entitySpec, entityUrn);

        return ResponseEntity.of(toRecordTemplates(List.of(UrnUtils.getUrn(entityUrn)), aspectNames).stream().findFirst());
    }

    @GetMapping(value = "/{entityName}/{entityUrn}/{aspectName}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Object> getAspect(@PathVariable("entityName") String entityName,
                                            @PathVariable("entityUrn") String entityUrn,
                                            @PathVariable("aspectName") String aspectName) {
        Authentication authentication = AuthenticationContext.getAuthentication();
        EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
        checkAuthorized(authentication, entitySpec, entityUrn);

        return ResponseEntity.of(toRecordTemplates(List.of(UrnUtils.getUrn(entityUrn)), Set.of(aspectName)).stream().findFirst()
                .flatMap(e -> e.getAspects().values().stream().findFirst()));
    }

    private void checkAuthorized(Authentication authentication, EntitySpec entitySpec) {
        checkAuthorized(authentication, entitySpec, null);
    }

    private void checkAuthorized(Authentication authentication, EntitySpec entitySpec, @Nullable String entityUrn) {
        String actorUrnStr = authentication.getActor().toUrnStr();
        DisjunctivePrivilegeGroup orGroup = new DisjunctivePrivilegeGroup(ImmutableList.of(new ConjunctivePrivilegeGroup(
                ImmutableList.of(PoliciesConfig.GET_ENTITY_PRIVILEGE.getType()))));

        List<Optional<com.datahub.authorization.EntitySpec>> resourceSpecs = List.of(Optional.of(
                new com.datahub.authorization.EntitySpec(entitySpec.getName(), entityUrn != null ? entityUrn : "")));
        if (restApiAuthorizationEnabled && !AuthUtil.isAuthorizedForResources(authorizationChain, actorUrnStr, resourceSpecs, orGroup)) {
            throw new UnauthorizedException(actorUrnStr + " is unauthorized to get entities.");
        }
    }

    private List<GenericEntity> toRecordTemplates(SearchEntityArray searchEntities, Set<String> aspectNames) {
        return toRecordTemplates(searchEntities.stream().map(SearchEntity::getEntity).collect(Collectors.toList()),
                aspectNames);
    }

    private List<GenericEntity> toRecordTemplates(List<Urn> urns, Set<String> aspectNames) {
        if (urns.isEmpty()) {
            return List.of();
        } else {
            Set<Urn> urnsSet = new HashSet<>(urns);
            Set<String> aspectNamesWithKey = Stream.concat(aspectNames.stream(),
                            urns.stream().map(u -> entityRegistry.getEntitySpec(u.getEntityType()).getKeyAspectName()))
                    .collect(Collectors.toSet());

            Map<Urn, List<RecordTemplate>> aspects = entityService.getLatestAspects(urnsSet, aspectNamesWithKey);

            return urns.stream().map(u ->
                            GenericEntity.builder()
                                    .urn(u.toString())
                                    .build(objectMapper, toAspectMap(aspects.getOrDefault(u, List.of()))))
                    .collect(Collectors.toList());
        }
    }

    private static Map<String, RecordTemplate> toAspectMap(List<RecordTemplate> aspects) {
        return aspects.stream().map(a ->
                Map.entry(a.schema().getName(), a))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}

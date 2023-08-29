package io.datahubproject.openapi.delegates;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.EntityService;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.openapi.dto.UpsertAspectRequest;
import io.datahubproject.openapi.dto.UrnResponseMap;
import io.datahubproject.openapi.entities.EntitiesController;
import com.datahub.authorization.AuthorizerChain;
import io.datahubproject.openapi.generated.BrowsePathsV2AspectRequestV2;
import io.datahubproject.openapi.generated.BrowsePathsV2AspectResponseV2;
import io.datahubproject.openapi.generated.DeprecationAspectRequestV2;
import io.datahubproject.openapi.generated.DeprecationAspectResponseV2;
import io.datahubproject.openapi.generated.DomainsAspectRequestV2;
import io.datahubproject.openapi.generated.DomainsAspectResponseV2;
import io.datahubproject.openapi.generated.GlobalTagsAspectRequestV2;
import io.datahubproject.openapi.generated.GlobalTagsAspectResponseV2;
import io.datahubproject.openapi.generated.GlossaryTermsAspectRequestV2;
import io.datahubproject.openapi.generated.GlossaryTermsAspectResponseV2;
import io.datahubproject.openapi.generated.OwnershipAspectRequestV2;
import io.datahubproject.openapi.generated.OwnershipAspectResponseV2;
import io.datahubproject.openapi.generated.SortOrder;
import io.datahubproject.openapi.generated.StatusAspectRequestV2;
import io.datahubproject.openapi.generated.StatusAspectResponseV2;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.util.OpenApiEntitiesUtil;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.metadata.models.EntitySpec;
import com.datahub.authorization.ResourceSpec;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.google.common.collect.ImmutableList;
import com.datahub.authorization.AuthUtil;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datahubproject.openapi.util.ReflectionCache.toLowerFirst;

public class EntityApiDelegateImpl<I, O, S> {
    final private EntityRegistry _entityRegistry;
    final private EntityService _entityService;
    final private SearchService _searchService;
    final private EntitiesController _v1Controller;
    final private AuthorizerChain _authorizationChain;

    final private boolean _restApiAuthorizationEnabled;
    final private Class<I> _reqClazz;
    final private Class<O> _respClazz;
    final private Class<S> _scrollRespClazz;

    final private StackWalker walker = StackWalker.getInstance();

    public EntityApiDelegateImpl(EntityService entityService, SearchService searchService, EntitiesController entitiesController,
                                 boolean restApiAuthorizationEnabled, AuthorizerChain authorizationChain,
                                 Class<I> reqClazz, Class<O> respClazz, Class<S> scrollRespClazz) {
        this._entityService = entityService;
        this._searchService = searchService;
        this._entityRegistry = entityService.getEntityRegistry();
        this._v1Controller = entitiesController;
        this._authorizationChain = authorizationChain;
        this._restApiAuthorizationEnabled = restApiAuthorizationEnabled;
        this._reqClazz = reqClazz;
        this._respClazz = respClazz;
        this._scrollRespClazz = scrollRespClazz;
    }

    public ResponseEntity<O> get(String urn, Boolean systemMetadata, List<String> aspects) {
        String[] requestedAspects = Optional.ofNullable(aspects).map(asp -> asp.stream().distinct().toArray(String[]::new)).orElse(null);
        ResponseEntity<UrnResponseMap> result = _v1Controller.getEntities(new String[]{urn}, requestedAspects);
        return ResponseEntity.of(OpenApiEntitiesUtil.convertEntity(Optional.ofNullable(result)
                .map(HttpEntity::getBody).orElse(null), _respClazz, systemMetadata));
    }

    public ResponseEntity<List<O>> create(List<I> body) {
        List<UpsertAspectRequest> aspects = body.stream()
                .flatMap(b -> OpenApiEntitiesUtil.convertEntityToUpsert(b, _reqClazz, _entityRegistry).stream())
                .collect(Collectors.toList());
        _v1Controller.postEntities(aspects);
        List<O> responses = body.stream()
                .map(req -> OpenApiEntitiesUtil.convertToResponse(req, _respClazz, _entityRegistry))
                .collect(Collectors.toList());
        return ResponseEntity.ok(responses);
    }

    public ResponseEntity<Void> delete(String urn) {
        _v1Controller.deleteEntities(new String[]{urn}, false);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    public ResponseEntity<Void> head(String urn) {
        try {
            Urn entityUrn = Urn.createFromString(urn);
            if (_entityService.exists(entityUrn)) {
                return new ResponseEntity<>(HttpStatus.NO_CONTENT);
            } else {
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public <A> ResponseEntity<A> getAspect(String urn, Boolean systemMetadata, String aspect, Class<O> entityRespClass,
                                           Class<A> aspectRespClazz) {
        String[] requestedAspects = new String[]{aspect};
        ResponseEntity<UrnResponseMap> result = _v1Controller.getEntities(new String[]{urn}, requestedAspects);
        return ResponseEntity.of(OpenApiEntitiesUtil.convertAspect(result.getBody(), aspect, entityRespClass, aspectRespClazz,
                systemMetadata));
    }

    public <AQ, AR> ResponseEntity<AR> createAspect(String urn, String aspectName, AQ body, Class<AQ> reqClazz, Class<AR> respClazz) {
        UpsertAspectRequest aspectUpsert = OpenApiEntitiesUtil.convertAspectToUpsert(urn, body, reqClazz);
        _v1Controller.postEntities(Stream.of(aspectUpsert).filter(Objects::nonNull).collect(Collectors.toList()));
        AR response = OpenApiEntitiesUtil.convertToResponseAspect(body, respClazz);
        return ResponseEntity.ok(response);
    }

    public ResponseEntity<Void> headAspect(String urn, String aspect) {
        try {
            Urn entityUrn = Urn.createFromString(urn);
            if (_entityService.exists(entityUrn, aspect)) {
                return new ResponseEntity<>(HttpStatus.NO_CONTENT);
            } else {
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public ResponseEntity<Void> deleteAspect(String urn, String aspect) {
        _entityService.deleteAspect(urn, aspect, Map.of(), false);
        _v1Controller.deleteEntities(new String[]{urn}, false);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    public ResponseEntity<DomainsAspectResponseV2> createDomains(DomainsAspectRequestV2 body, String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return createAspect(urn, methodNameToAspectName(methodName), body, DomainsAspectRequestV2.class, DomainsAspectResponseV2.class);
    }

    public ResponseEntity<GlobalTagsAspectResponseV2> createGlobalTags(GlobalTagsAspectRequestV2 body, String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return createAspect(urn, methodNameToAspectName(methodName), body, GlobalTagsAspectRequestV2.class, GlobalTagsAspectResponseV2.class);
    }

    public ResponseEntity<GlossaryTermsAspectResponseV2> createGlossaryTerms(GlossaryTermsAspectRequestV2 body, String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return createAspect(urn, methodNameToAspectName(methodName), body, GlossaryTermsAspectRequestV2.class, GlossaryTermsAspectResponseV2.class);
    }

    public ResponseEntity<OwnershipAspectResponseV2> createOwnership(OwnershipAspectRequestV2 body, String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return createAspect(urn, methodNameToAspectName(methodName), body, OwnershipAspectRequestV2.class, OwnershipAspectResponseV2.class);
    }

    public ResponseEntity<StatusAspectResponseV2> createStatus(StatusAspectRequestV2 body, String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return createAspect(urn, methodNameToAspectName(methodName), body, StatusAspectRequestV2.class, StatusAspectResponseV2.class);
    }

    public ResponseEntity<Void> deleteDomains(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return deleteAspect(urn, methodNameToAspectName(methodName));
    }

    public ResponseEntity<Void> deleteGlobalTags(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return deleteAspect(urn, methodNameToAspectName(methodName));
    }

    public ResponseEntity<Void> deleteGlossaryTerms(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return deleteAspect(urn, methodNameToAspectName(methodName));
    }

    public ResponseEntity<Void> deleteOwnership(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return deleteAspect(urn, methodNameToAspectName(methodName));
    }

    public ResponseEntity<Void> deleteStatus(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return deleteAspect(urn, methodNameToAspectName(methodName));
    }

    public ResponseEntity<DomainsAspectResponseV2> getDomains(String urn, Boolean systemMetadata) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return getAspect(urn, systemMetadata, methodNameToAspectName(methodName), _respClazz,
                DomainsAspectResponseV2.class);
    }

    public ResponseEntity<GlobalTagsAspectResponseV2> getGlobalTags(String urn, Boolean systemMetadata) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return getAspect(urn, systemMetadata, methodNameToAspectName(methodName), _respClazz,
                GlobalTagsAspectResponseV2.class);
    }

    public ResponseEntity<GlossaryTermsAspectResponseV2> getGlossaryTerms(String urn, Boolean systemMetadata) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return getAspect(urn, systemMetadata, methodNameToAspectName(methodName), _respClazz,
                GlossaryTermsAspectResponseV2.class);
    }

    public ResponseEntity<OwnershipAspectResponseV2> getOwnership(String urn, Boolean systemMetadata) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return getAspect(urn, systemMetadata, methodNameToAspectName(methodName), _respClazz,
                OwnershipAspectResponseV2.class);
    }

    public ResponseEntity<StatusAspectResponseV2> getStatus(String urn, Boolean systemMetadata) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return getAspect(urn, systemMetadata, methodNameToAspectName(methodName), _respClazz,
                StatusAspectResponseV2.class);
    }

    public ResponseEntity<Void> headDomains(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return headAspect(urn, methodNameToAspectName(methodName));
    }

    public ResponseEntity<Void> headGlobalTags(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return headAspect(urn, methodNameToAspectName(methodName));
    }

    public ResponseEntity<Void> headGlossaryTerms(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return headAspect(urn, methodNameToAspectName(methodName));
    }

    public ResponseEntity<Void> headOwnership(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return headAspect(urn, methodNameToAspectName(methodName));
    }

    public ResponseEntity<Void> headStatus(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return headAspect(urn, methodNameToAspectName(methodName));
    }

    protected static String methodNameToAspectName(String methodName) {
        return toLowerFirst(methodName.replaceFirst("^(get|head|delete|create)", ""));
    }

    public ResponseEntity<Void> deleteDeprecation(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return deleteAspect(urn, methodNameToAspectName(methodName));
    }

    public ResponseEntity<Void> deleteBrowsePathsV2(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return deleteAspect(urn, methodNameToAspectName(methodName));
    }

    public ResponseEntity<DeprecationAspectResponseV2> getDeprecation(String urn, @Valid Boolean systemMetadata) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return getAspect(urn, systemMetadata, methodNameToAspectName(methodName), _respClazz,
                DeprecationAspectResponseV2.class);
    }

    public ResponseEntity<Void> headDeprecation(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return headAspect(urn, methodNameToAspectName(methodName));
    }

    public ResponseEntity<DeprecationAspectResponseV2> createDeprecation(@Valid DeprecationAspectRequestV2 body, String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return createAspect(urn, methodNameToAspectName(methodName), body, DeprecationAspectRequestV2.class,
                DeprecationAspectResponseV2.class);
    }

    public ResponseEntity<Void> headBrowsePathsV2(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return headAspect(urn, methodNameToAspectName(methodName));
    }

    public ResponseEntity<BrowsePathsV2AspectResponseV2> getBrowsePathsV2(String urn, @Valid Boolean systemMetadata) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return getAspect(urn, systemMetadata, methodNameToAspectName(methodName), _respClazz,
                BrowsePathsV2AspectResponseV2.class);
    }

    public ResponseEntity<BrowsePathsV2AspectResponseV2> createBrowsePathsV2(@Valid BrowsePathsV2AspectRequestV2 body, String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return createAspect(urn, methodNameToAspectName(methodName), body, BrowsePathsV2AspectRequestV2.class,
                BrowsePathsV2AspectResponseV2.class);
    }

    public ResponseEntity<S> scroll(@Valid Boolean systemMetadata, @Valid List<String> aspects, @Min(1) @Valid Integer count,
                                    @Valid String scrollId, @Valid List<String> sort, @Valid SortOrder sortOrder, @Valid String query) {

        Authentication authentication = AuthenticationContext.getAuthentication();
        EntitySpec entitySpec = OpenApiEntitiesUtil.responseClassToEntitySpec(_entityRegistry, _respClazz);
        checkScrollAuthorized(authentication, entitySpec);

        // TODO multi-field sort
        SortCriterion sortCriterion = new SortCriterion();
        sortCriterion.setField(Optional.ofNullable(sort).map(s -> s.get(0)).orElse("urn"));
        sortCriterion.setOrder(com.linkedin.metadata.query.filter.SortOrder.valueOf(Optional.ofNullable(sortOrder)
                .map(Enum::name).orElse("ASCENDING")));

        SearchFlags searchFlags = new SearchFlags()
                .setFulltext(false)
                .setSkipAggregates(true)
                .setSkipHighlighting(true);

        ScrollResult result = _searchService.scrollAcrossEntities(
                List.of(entitySpec.getName()),
                query, null, sortCriterion, scrollId, null, count, searchFlags);

        String[] urns = result.getEntities().stream()
                .map(SearchEntity::getEntity)
                .map(Urn::toString)
                .toArray(String[]::new);
        String[] requestedAspects = Optional.ofNullable(aspects)
                .map(asp -> asp.stream().distinct().toArray(String[]::new))
                .orElse(null);
        List<O> entities = Optional.ofNullable(_v1Controller.getEntities(urns, requestedAspects).getBody())
                .map(body -> body.getResponses().entrySet())
                .map(entries -> OpenApiEntitiesUtil.convertEntities(entries, _respClazz, systemMetadata))
                .orElse(List.of());

        return ResponseEntity.of(OpenApiEntitiesUtil.convertToScrollResponse(_scrollRespClazz, result.getScrollId(), entities));
    }

    private void checkScrollAuthorized(Authentication authentication, EntitySpec entitySpec) {
        String actorUrnStr = authentication.getActor().toUrnStr();
        DisjunctivePrivilegeGroup orGroup = new DisjunctivePrivilegeGroup(ImmutableList.of(new ConjunctivePrivilegeGroup(
                ImmutableList.of(PoliciesConfig.GET_ENTITY_PRIVILEGE.getType()))));

        List<Optional<ResourceSpec>> resourceSpecs = List.of(Optional.of(new ResourceSpec(entitySpec.getName(), "")));
        if (_restApiAuthorizationEnabled && !AuthUtil.isAuthorizedForResources(_authorizationChain, actorUrnStr, resourceSpecs, orGroup)) {
            throw new UnauthorizedException(actorUrnStr + " is unauthorized to get entities.");
        }
    }
}

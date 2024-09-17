package io.datahubproject.openapi.v2.delegates;

import static com.linkedin.metadata.authorization.ApiOperation.EXISTS;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static io.datahubproject.openapi.util.ReflectionCache.toLowerFirst;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.dto.UpsertAspectRequest;
import io.datahubproject.openapi.dto.UrnResponseMap;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.generated.BrowsePathsV2AspectRequestV2;
import io.datahubproject.openapi.generated.BrowsePathsV2AspectResponseV2;
import io.datahubproject.openapi.generated.BusinessAttributeInfoAspectRequestV2;
import io.datahubproject.openapi.generated.BusinessAttributeInfoAspectResponseV2;
import io.datahubproject.openapi.generated.ChartInfoAspectRequestV2;
import io.datahubproject.openapi.generated.ChartInfoAspectResponseV2;
import io.datahubproject.openapi.generated.DataProductPropertiesAspectRequestV2;
import io.datahubproject.openapi.generated.DataProductPropertiesAspectResponseV2;
import io.datahubproject.openapi.generated.DatasetPropertiesAspectRequestV2;
import io.datahubproject.openapi.generated.DatasetPropertiesAspectResponseV2;
import io.datahubproject.openapi.generated.DeprecationAspectRequestV2;
import io.datahubproject.openapi.generated.DeprecationAspectResponseV2;
import io.datahubproject.openapi.generated.DomainsAspectRequestV2;
import io.datahubproject.openapi.generated.DomainsAspectResponseV2;
import io.datahubproject.openapi.generated.DynamicFormAssignmentAspectRequestV2;
import io.datahubproject.openapi.generated.DynamicFormAssignmentAspectResponseV2;
import io.datahubproject.openapi.generated.EditableChartPropertiesAspectRequestV2;
import io.datahubproject.openapi.generated.EditableChartPropertiesAspectResponseV2;
import io.datahubproject.openapi.generated.EditableDatasetPropertiesAspectRequestV2;
import io.datahubproject.openapi.generated.EditableDatasetPropertiesAspectResponseV2;
import io.datahubproject.openapi.generated.FormInfoAspectRequestV2;
import io.datahubproject.openapi.generated.FormInfoAspectResponseV2;
import io.datahubproject.openapi.generated.FormsAspectRequestV2;
import io.datahubproject.openapi.generated.FormsAspectResponseV2;
import io.datahubproject.openapi.generated.GlobalTagsAspectRequestV2;
import io.datahubproject.openapi.generated.GlobalTagsAspectResponseV2;
import io.datahubproject.openapi.generated.GlossaryTermsAspectRequestV2;
import io.datahubproject.openapi.generated.GlossaryTermsAspectResponseV2;
import io.datahubproject.openapi.generated.InstitutionalMemoryAspectRequestV2;
import io.datahubproject.openapi.generated.InstitutionalMemoryAspectResponseV2;
import io.datahubproject.openapi.generated.OwnershipAspectRequestV2;
import io.datahubproject.openapi.generated.OwnershipAspectResponseV2;
import io.datahubproject.openapi.generated.SortOrder;
import io.datahubproject.openapi.generated.StatusAspectRequestV2;
import io.datahubproject.openapi.generated.StatusAspectResponseV2;
import io.datahubproject.openapi.util.OpenApiEntitiesUtil;
import io.datahubproject.openapi.v1.entities.EntitiesController;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@Slf4j
@Accessors(chain = true)
public class EntityApiDelegateImpl<I, O, S> {
  private final OperationContext systemOperationContext;
  private final EntityRegistry _entityRegistry;
  private final EntityService<?> _entityService;
  private final SearchService _searchService;
  private final EntitiesController _v1Controller;
  private final AuthorizerChain _authorizationChain;
  private final Class<I> _reqClazz;
  private final Class<O> _respClazz;
  private final Class<S> _scrollRespClazz;
  @Setter @Getter private HttpServletRequest request;

  private static final String BUSINESS_ATTRIBUTE_ERROR_MESSAGE =
      "business attribute is disabled, enable it using featureflag : BUSINESS_ATTRIBUTE_ENTITY_ENABLED";
  private final StackWalker walker = StackWalker.getInstance();

  public EntityApiDelegateImpl(
      OperationContext systemOperationContext,
      HttpServletRequest request,
      EntityService<?> entityService,
      SearchService searchService,
      EntitiesController entitiesController,
      AuthorizerChain authorizationChain,
      Class<I> reqClazz,
      Class<O> respClazz,
      Class<S> scrollRespClazz) {
    this.systemOperationContext = systemOperationContext;
    this.request = request;
    this._entityService = entityService;
    this._searchService = searchService;
    this._entityRegistry = systemOperationContext.getEntityRegistry();
    this._v1Controller = entitiesController;
    this._authorizationChain = authorizationChain;
    this._reqClazz = reqClazz;
    this._respClazz = respClazz;
    this._scrollRespClazz = scrollRespClazz;
  }

  public ResponseEntity<O> get(String urn, Boolean systemMetadata, List<String> aspects) {
    if (checkBusinessAttributeFlagFromUrn(urn)) {
      throw new UnsupportedOperationException(BUSINESS_ATTRIBUTE_ERROR_MESSAGE);
    }
    String[] requestedAspects =
        Optional.ofNullable(aspects)
            .map(asp -> asp.stream().distinct().toArray(String[]::new))
            .orElse(null);
    ResponseEntity<UrnResponseMap> result =
        _v1Controller.getEntities(request, new String[] {urn}, requestedAspects);
    return ResponseEntity.of(
        OpenApiEntitiesUtil.convertEntity(
            Optional.ofNullable(result).map(HttpEntity::getBody).orElse(null),
            _respClazz,
            systemMetadata));
  }

  public ResponseEntity<List<O>> create(
      List<I> body,
      @Nullable Boolean createIfNotExists,
      @Nullable Boolean createEntityIfNotExists) {
    List<UpsertAspectRequest> aspects =
        body.stream()
            .flatMap(
                b ->
                    OpenApiEntitiesUtil.convertEntityToUpsert(b, _reqClazz, _entityRegistry)
                        .stream())
            .collect(Collectors.toList());

    Optional<UpsertAspectRequest> aspect = aspects.stream().findFirst();
    if (aspect.isPresent()) {
      String entityType = aspect.get().getEntityType();
      if (checkBusinessAttributeFlagFromEntityType(entityType)) {
        throw new UnsupportedOperationException(BUSINESS_ATTRIBUTE_ERROR_MESSAGE);
      }
    }
    _v1Controller.postEntities(request, aspects, false, createIfNotExists, createEntityIfNotExists);
    List<O> responses =
        body.stream()
            .map(req -> OpenApiEntitiesUtil.convertToResponse(req, _respClazz, _entityRegistry))
            .collect(Collectors.toList());
    return ResponseEntity.ok(responses);
  }

  public ResponseEntity<Void> delete(String urn) {
    if (checkBusinessAttributeFlagFromUrn(urn)) {
      throw new UnsupportedOperationException(BUSINESS_ATTRIBUTE_ERROR_MESSAGE);
    }
    _v1Controller.deleteEntities(request, new String[] {urn}, false, false);
    return new ResponseEntity<>(HttpStatus.OK);
  }

  public ResponseEntity<Void> head(String urn) {
    if (checkBusinessAttributeFlagFromUrn(urn)) {
      throw new UnsupportedOperationException(BUSINESS_ATTRIBUTE_ERROR_MESSAGE);
    }
    try {
      Urn entityUrn = Urn.createFromString(urn);
      final Authentication auth = AuthenticationContext.getAuthentication();
      OperationContext opContext =
          OperationContext.asSession(
              systemOperationContext,
              RequestContext.builder()
                  .buildOpenapi(
                      auth.getActor().toUrnStr(), request, "head", entityUrn.getEntityType()),
              _authorizationChain,
              auth,
              true);

      if (!AuthUtil.isAPIAuthorizedEntityUrns(opContext, EXISTS, List.of(entityUrn))) {
        throw new UnauthorizedException(
            auth.getActor().toUrnStr() + " is unauthorized to check existence of entities.");
      }

      if (_entityService.exists(opContext, entityUrn, true)) {
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
      } else {
        return new ResponseEntity<>(HttpStatus.NOT_FOUND);
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public <A> ResponseEntity<A> getAspect(
      String urn,
      Boolean systemMetadata,
      String aspect,
      Class<O> entityRespClass,
      Class<A> aspectRespClazz) {
    String[] requestedAspects = new String[] {aspect};
    ResponseEntity<UrnResponseMap> result =
        _v1Controller.getEntities(request, new String[] {urn}, requestedAspects);
    return ResponseEntity.of(
        OpenApiEntitiesUtil.convertAspect(
            result.getBody(), aspect, entityRespClass, aspectRespClazz, systemMetadata));
  }

  public <AQ, AR> ResponseEntity<AR> createAspect(
      String urn,
      String aspectName,
      AQ body,
      Class<AQ> reqClazz,
      Class<AR> respClazz,
      @Nullable Boolean createIfNotExists,
      @Nullable Boolean createEntityIfNotExists) {
    UpsertAspectRequest aspectUpsert =
        OpenApiEntitiesUtil.convertAspectToUpsert(urn, body, reqClazz);
    _v1Controller.postEntities(
        request,
        Stream.of(aspectUpsert).filter(Objects::nonNull).collect(Collectors.toList()),
        false,
        createIfNotExists,
        createEntityIfNotExists);
    AR response = OpenApiEntitiesUtil.convertToResponseAspect(body, respClazz);
    return ResponseEntity.ok(response);
  }

  public ResponseEntity<Void> headAspect(String urn, String aspect) {
    try {
      Urn entityUrn = Urn.createFromString(urn);

      final Authentication auth = AuthenticationContext.getAuthentication();
      OperationContext opContext =
          OperationContext.asSession(
              systemOperationContext,
              RequestContext.builder()
                  .buildOpenapi(
                      auth.getActor().toUrnStr(), request, "headAspect", entityUrn.getEntityType()),
              _authorizationChain,
              auth,
              true);

      if (!AuthUtil.isAPIAuthorizedEntityUrns(opContext, EXISTS, List.of(entityUrn))) {
        throw new UnauthorizedException(
            auth.getActor().toUrnStr() + " is unauthorized to check existence of entities.");
      }

      if (_entityService.exists(opContext, entityUrn, aspect, true)) {
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
      } else {
        return new ResponseEntity<>(HttpStatus.NOT_FOUND);
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public ResponseEntity<Void> deleteAspect(String urn, String aspect) {
    final Authentication auth = AuthenticationContext.getAuthentication();
    Urn entityUrn = UrnUtils.getUrn(urn);
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    auth.getActor().toUrnStr(), request, "deleteAspect", entityUrn.getEntityType()),
            _authorizationChain,
            auth,
            true);
    _entityService.deleteAspect(opContext, urn, aspect, Map.of(), false);
    _v1Controller.deleteEntities(request, new String[] {urn}, false, false);
    return new ResponseEntity<>(HttpStatus.OK);
  }

  public ResponseEntity<DomainsAspectResponseV2> createDomains(
      DomainsAspectRequestV2 body,
      String urn,
      @Nullable Boolean createIfNotExists,
      @Nullable Boolean createEntityIfNotExists) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return createAspect(
        urn,
        methodNameToAspectName(methodName),
        body,
        DomainsAspectRequestV2.class,
        DomainsAspectResponseV2.class,
        createIfNotExists,
        createEntityIfNotExists);
  }

  public ResponseEntity<GlobalTagsAspectResponseV2> createGlobalTags(
      GlobalTagsAspectRequestV2 body,
      String urn,
      @Nullable Boolean createIfNotExists,
      @Nullable Boolean createEntityIfNotExists) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return createAspect(
        urn,
        methodNameToAspectName(methodName),
        body,
        GlobalTagsAspectRequestV2.class,
        GlobalTagsAspectResponseV2.class,
        createIfNotExists,
        createEntityIfNotExists);
  }

  public ResponseEntity<GlossaryTermsAspectResponseV2> createGlossaryTerms(
      GlossaryTermsAspectRequestV2 body,
      String urn,
      @Nullable Boolean createIfNotExists,
      @Nullable Boolean createEntityIfNotExists) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return createAspect(
        urn,
        methodNameToAspectName(methodName),
        body,
        GlossaryTermsAspectRequestV2.class,
        GlossaryTermsAspectResponseV2.class,
        createIfNotExists,
        createEntityIfNotExists);
  }

  public ResponseEntity<OwnershipAspectResponseV2> createOwnership(
      OwnershipAspectRequestV2 body,
      String urn,
      @Nullable Boolean createIfNotExists,
      @Nullable Boolean createEntityIfNotExists) {
    if (checkBusinessAttributeFlagFromUrn(urn)) {
      throw new UnsupportedOperationException(BUSINESS_ATTRIBUTE_ERROR_MESSAGE);
    }
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return createAspect(
        urn,
        methodNameToAspectName(methodName),
        body,
        OwnershipAspectRequestV2.class,
        OwnershipAspectResponseV2.class,
        createIfNotExists,
        createEntityIfNotExists);
  }

  public ResponseEntity<StatusAspectResponseV2> createStatus(
      StatusAspectRequestV2 body,
      String urn,
      @Nullable Boolean createIfNotExists,
      @Nullable Boolean createEntityIfNotExists) {
    if (checkBusinessAttributeFlagFromUrn(urn)) {
      throw new UnsupportedOperationException(BUSINESS_ATTRIBUTE_ERROR_MESSAGE);
    }
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return createAspect(
        urn,
        methodNameToAspectName(methodName),
        body,
        StatusAspectRequestV2.class,
        StatusAspectResponseV2.class,
        createIfNotExists,
        createEntityIfNotExists);
  }

  public ResponseEntity<Void> deleteDomains(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return deleteAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> deleteGlobalTags(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return deleteAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> deleteGlossaryTerms(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return deleteAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> deleteOwnership(String urn) {
    if (checkBusinessAttributeFlagFromUrn(urn)) {
      throw new UnsupportedOperationException(BUSINESS_ATTRIBUTE_ERROR_MESSAGE);
    }
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return deleteAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> deleteStatus(String urn) {
    if (checkBusinessAttributeFlagFromUrn(urn)) {
      throw new UnsupportedOperationException(BUSINESS_ATTRIBUTE_ERROR_MESSAGE);
    }
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return deleteAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<DomainsAspectResponseV2> getDomains(String urn, Boolean systemMetadata) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return getAspect(
        urn,
        systemMetadata,
        methodNameToAspectName(methodName),
        _respClazz,
        DomainsAspectResponseV2.class);
  }

  public ResponseEntity<GlobalTagsAspectResponseV2> getGlobalTags(
      String urn, Boolean systemMetadata) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return getAspect(
        urn,
        systemMetadata,
        methodNameToAspectName(methodName),
        _respClazz,
        GlobalTagsAspectResponseV2.class);
  }

  public ResponseEntity<GlossaryTermsAspectResponseV2> getGlossaryTerms(
      String urn, Boolean systemMetadata) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return getAspect(
        urn,
        systemMetadata,
        methodNameToAspectName(methodName),
        _respClazz,
        GlossaryTermsAspectResponseV2.class);
  }

  public ResponseEntity<OwnershipAspectResponseV2> getOwnership(
      String urn, Boolean systemMetadata) {
    if (checkBusinessAttributeFlagFromUrn(urn)) {
      throw new UnsupportedOperationException(BUSINESS_ATTRIBUTE_ERROR_MESSAGE);
    }
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return getAspect(
        urn,
        systemMetadata,
        methodNameToAspectName(methodName),
        _respClazz,
        OwnershipAspectResponseV2.class);
  }

  public ResponseEntity<StatusAspectResponseV2> getStatus(String urn, Boolean systemMetadata) {
    if (checkBusinessAttributeFlagFromUrn(urn)) {
      throw new UnsupportedOperationException(BUSINESS_ATTRIBUTE_ERROR_MESSAGE);
    }
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return getAspect(
        urn,
        systemMetadata,
        methodNameToAspectName(methodName),
        _respClazz,
        StatusAspectResponseV2.class);
  }

  public ResponseEntity<Void> headDomains(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return headAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> headGlobalTags(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return headAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> headGlossaryTerms(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return headAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> headOwnership(String urn) {
    if (checkBusinessAttributeFlagFromUrn(urn)) {
      throw new UnsupportedOperationException(BUSINESS_ATTRIBUTE_ERROR_MESSAGE);
    }
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return headAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> headStatus(String urn) {
    if (checkBusinessAttributeFlagFromUrn(urn)) {
      throw new UnsupportedOperationException(BUSINESS_ATTRIBUTE_ERROR_MESSAGE);
    }
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return headAspect(urn, methodNameToAspectName(methodName));
  }

  protected static String methodNameToAspectName(String methodName) {
    return toLowerFirst(methodName.replaceFirst("^(get|head|delete|create)", ""));
  }

  public ResponseEntity<Void> deleteDeprecation(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return deleteAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> deleteBrowsePathsV2(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return deleteAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<DeprecationAspectResponseV2> getDeprecation(
      String urn, @Valid Boolean systemMetadata) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return getAspect(
        urn,
        systemMetadata,
        methodNameToAspectName(methodName),
        _respClazz,
        DeprecationAspectResponseV2.class);
  }

  public ResponseEntity<Void> headDeprecation(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return headAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<DeprecationAspectResponseV2> createDeprecation(
      @Valid DeprecationAspectRequestV2 body,
      String urn,
      @Nullable Boolean createIfNotExists,
      @Nullable Boolean createEntityIfNotExists) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return createAspect(
        urn,
        methodNameToAspectName(methodName),
        body,
        DeprecationAspectRequestV2.class,
        DeprecationAspectResponseV2.class,
        createIfNotExists,
        createEntityIfNotExists);
  }

  public ResponseEntity<Void> headBrowsePathsV2(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return headAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<BrowsePathsV2AspectResponseV2> getBrowsePathsV2(
      String urn, @Valid Boolean systemMetadata) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return getAspect(
        urn,
        systemMetadata,
        methodNameToAspectName(methodName),
        _respClazz,
        BrowsePathsV2AspectResponseV2.class);
  }

  public ResponseEntity<BrowsePathsV2AspectResponseV2> createBrowsePathsV2(
      @Valid BrowsePathsV2AspectRequestV2 body,
      String urn,
      @Nullable Boolean createIfNotExists,
      @Nullable Boolean createEntityIfNotExists) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return createAspect(
        urn,
        methodNameToAspectName(methodName),
        body,
        BrowsePathsV2AspectRequestV2.class,
        BrowsePathsV2AspectResponseV2.class,
        createIfNotExists,
        createEntityIfNotExists);
  }

  public ResponseEntity<S> scroll(
      @Valid Boolean systemMetadata,
      @Valid List<String> aspects,
      @Min(1) @Valid Integer count,
      @Valid String scrollId,
      @Valid List<String> sort,
      @Valid SortOrder sortOrder,
      @Valid String query) {

    SearchFlags searchFlags =
        new SearchFlags().setFulltext(false).setSkipAggregates(true).setSkipHighlighting(true);
    com.linkedin.metadata.models.EntitySpec entitySpec =
        OpenApiEntitiesUtil.responseClassToEntitySpec(_entityRegistry, _respClazz);

    Authentication authentication = AuthenticationContext.getAuthentication();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "scroll", entitySpec.getName()),
            _authorizationChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorizedEntityType(opContext, READ, entitySpec.getName())) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to search entities.");
    }

    List<SortCriterion> sortCriteria =
        Optional.ofNullable(sort).orElse(Collections.singletonList("urn")).stream()
            .map(
                sortField -> {
                  SortCriterion sortCriterion = new SortCriterion();
                  sortCriterion.setField(sortField);
                  sortCriterion.setOrder(
                      com.linkedin.metadata.query.filter.SortOrder.valueOf(
                          Optional.ofNullable(sortOrder).map(Enum::name).orElse("ASCENDING")));
                  return sortCriterion;
                })
            .collect(Collectors.toList());

    ScrollResult result =
        _searchService.scrollAcrossEntities(
            opContext.withSearchFlags(flags -> searchFlags),
            List.of(entitySpec.getName()),
            query,
            null,
            sortCriteria,
            scrollId,
            null,
            count);

    if (!AuthUtil.isAPIAuthorizedResult(opContext, result)) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + READ + " entities.");
    }

    String[] urns =
        result.getEntities().stream()
            .map(SearchEntity::getEntity)
            .map(Urn::toString)
            .toArray(String[]::new);
    String[] requestedAspects =
        Optional.ofNullable(aspects)
            .map(asp -> asp.stream().distinct().toArray(String[]::new))
            .orElse(null);
    List<O> entities =
        Optional.ofNullable(_v1Controller.getEntities(request, urns, requestedAspects).getBody())
            .map(body -> body.getResponses().entrySet())
            .map(
                entries -> OpenApiEntitiesUtil.convertEntities(entries, _respClazz, systemMetadata))
            .orElse(List.of());

    return ResponseEntity.of(
        OpenApiEntitiesUtil.convertToScrollResponse(
            _scrollRespClazz, result.getScrollId(), entities));
  }

  public ResponseEntity<DatasetPropertiesAspectResponseV2> createDatasetProperties(
      @Valid DatasetPropertiesAspectRequestV2 body,
      String urn,
      @Nullable Boolean createIfNotExists,
      @Nullable Boolean createEntityIfNotExists) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return createAspect(
        urn,
        methodNameToAspectName(methodName),
        body,
        DatasetPropertiesAspectRequestV2.class,
        DatasetPropertiesAspectResponseV2.class,
        createIfNotExists,
        createEntityIfNotExists);
  }

  public ResponseEntity<EditableDatasetPropertiesAspectResponseV2> createEditableDatasetProperties(
      @Valid EditableDatasetPropertiesAspectRequestV2 body,
      String urn,
      @Nullable Boolean createIfNotExists,
      @Nullable Boolean createEntityIfNotExists) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return createAspect(
        urn,
        methodNameToAspectName(methodName),
        body,
        EditableDatasetPropertiesAspectRequestV2.class,
        EditableDatasetPropertiesAspectResponseV2.class,
        createIfNotExists,
        createEntityIfNotExists);
  }

  public ResponseEntity<InstitutionalMemoryAspectResponseV2> createInstitutionalMemory(
      @Valid InstitutionalMemoryAspectRequestV2 body,
      String urn,
      @Nullable Boolean createIfNotExists,
      @Nullable Boolean createEntityIfNotExists) {
    if (checkBusinessAttributeFlagFromUrn(urn)) {
      throw new UnsupportedOperationException(BUSINESS_ATTRIBUTE_ERROR_MESSAGE);
    }
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return createAspect(
        urn,
        methodNameToAspectName(methodName),
        body,
        InstitutionalMemoryAspectRequestV2.class,
        InstitutionalMemoryAspectResponseV2.class,
        createIfNotExists,
        createEntityIfNotExists);
  }

  public ResponseEntity<ChartInfoAspectResponseV2> createChartInfo(
      @Valid ChartInfoAspectRequestV2 body,
      String urn,
      @Nullable Boolean createIfNotExists,
      @Nullable Boolean createEntityIfNotExists) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return createAspect(
        urn,
        methodNameToAspectName(methodName),
        body,
        ChartInfoAspectRequestV2.class,
        ChartInfoAspectResponseV2.class,
        createIfNotExists,
        createEntityIfNotExists);
  }

  public ResponseEntity<EditableChartPropertiesAspectResponseV2> createEditableChartProperties(
      @Valid EditableChartPropertiesAspectRequestV2 body,
      String urn,
      @Nullable Boolean createIfNotExists,
      @Nullable Boolean createEntityIfNotExists) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return createAspect(
        urn,
        methodNameToAspectName(methodName),
        body,
        EditableChartPropertiesAspectRequestV2.class,
        EditableChartPropertiesAspectResponseV2.class,
        createIfNotExists,
        createEntityIfNotExists);
  }

  public ResponseEntity<DataProductPropertiesAspectResponseV2> createDataProductProperties(
      @Valid DataProductPropertiesAspectRequestV2 body,
      String urn,
      @Nullable Boolean createIfNotExists,
      @Nullable Boolean createEntityIfNotExists) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return createAspect(
        urn,
        methodNameToAspectName(methodName),
        body,
        DataProductPropertiesAspectRequestV2.class,
        DataProductPropertiesAspectResponseV2.class,
        createIfNotExists,
        createEntityIfNotExists);
  }

  public ResponseEntity<Void> deleteDatasetProperties(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return deleteAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> deleteEditableDatasetProperties(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return deleteAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> deleteInstitutionalMemory(String urn) {
    if (checkBusinessAttributeFlagFromUrn(urn)) {
      throw new UnsupportedOperationException(BUSINESS_ATTRIBUTE_ERROR_MESSAGE);
    }
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return deleteAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> deleteChartInfo(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return deleteAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<DatasetPropertiesAspectResponseV2> getDatasetProperties(
      String urn, Boolean systemMetadata) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return getAspect(
        urn,
        systemMetadata,
        methodNameToAspectName(methodName),
        _respClazz,
        DatasetPropertiesAspectResponseV2.class);
  }

  public ResponseEntity<EditableDatasetPropertiesAspectResponseV2> getEditableDatasetProperties(
      String urn, Boolean systemMetadata) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return getAspect(
        urn,
        systemMetadata,
        methodNameToAspectName(methodName),
        _respClazz,
        EditableDatasetPropertiesAspectResponseV2.class);
  }

  public ResponseEntity<InstitutionalMemoryAspectResponseV2> getInstitutionalMemory(
      String urn, Boolean systemMetadata) {
    if (checkBusinessAttributeFlagFromUrn(urn)) {
      throw new UnsupportedOperationException(BUSINESS_ATTRIBUTE_ERROR_MESSAGE);
    }
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return getAspect(
        urn,
        systemMetadata,
        methodNameToAspectName(methodName),
        _respClazz,
        InstitutionalMemoryAspectResponseV2.class);
  }

  public ResponseEntity<EditableChartPropertiesAspectResponseV2> getEditableChartProperties(
      String urn, Boolean systemMetadata) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return getAspect(
        urn,
        systemMetadata,
        methodNameToAspectName(methodName),
        _respClazz,
        EditableChartPropertiesAspectResponseV2.class);
  }

  public ResponseEntity<ChartInfoAspectResponseV2> getChartInfo(
      String urn, Boolean systemMetadata) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return getAspect(
        urn,
        systemMetadata,
        methodNameToAspectName(methodName),
        _respClazz,
        ChartInfoAspectResponseV2.class);
  }

  public ResponseEntity<DataProductPropertiesAspectResponseV2> getDataProductProperties(
      String urn, Boolean systemMetadata) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return getAspect(
        urn,
        systemMetadata,
        methodNameToAspectName(methodName),
        _respClazz,
        DataProductPropertiesAspectResponseV2.class);
  }

  public ResponseEntity<Void> headDatasetProperties(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return headAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> headEditableDatasetProperties(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return headAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> headInstitutionalMemory(String urn) {
    if (checkBusinessAttributeFlagFromUrn(urn)) {
      throw new UnsupportedOperationException(BUSINESS_ATTRIBUTE_ERROR_MESSAGE);
    }
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return headAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> headDataProductProperties(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return headAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> headEditableChartProperties(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return headAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> headChartInfo(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return headAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> deleteEditableChartProperties(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return deleteAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> deleteDataProductProperties(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return deleteAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<FormsAspectResponseV2> createForms(
      FormsAspectRequestV2 body,
      String urn,
      @Nullable Boolean createIfNotExists,
      @Nullable Boolean createEntityIfNotExists) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return createAspect(
        urn,
        methodNameToAspectName(methodName),
        body,
        FormsAspectRequestV2.class,
        FormsAspectResponseV2.class,
        createIfNotExists,
        createEntityIfNotExists);
  }

  public ResponseEntity<Void> deleteForms(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return deleteAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<FormsAspectResponseV2> getForms(
      String urn, @jakarta.validation.Valid Boolean systemMetadata) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return getAspect(
        urn,
        systemMetadata,
        methodNameToAspectName(methodName),
        _respClazz,
        FormsAspectResponseV2.class);
  }

  public ResponseEntity<Void> headForms(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return headAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<DynamicFormAssignmentAspectResponseV2> createDynamicFormAssignment(
      DynamicFormAssignmentAspectRequestV2 body,
      String urn,
      @Nullable Boolean createIfNotExists,
      @Nullable Boolean createEntityIfNotExists) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return createAspect(
        urn,
        methodNameToAspectName(methodName),
        body,
        DynamicFormAssignmentAspectRequestV2.class,
        DynamicFormAssignmentAspectResponseV2.class,
        createIfNotExists,
        createEntityIfNotExists);
  }

  public ResponseEntity<FormInfoAspectResponseV2> createFormInfo(
      FormInfoAspectRequestV2 body,
      String urn,
      @Nullable Boolean createIfNotExists,
      @Nullable Boolean createEntityIfNotExists) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return createAspect(
        urn,
        methodNameToAspectName(methodName),
        body,
        FormInfoAspectRequestV2.class,
        FormInfoAspectResponseV2.class,
        createIfNotExists,
        createEntityIfNotExists);
  }

  public ResponseEntity<Void> deleteDynamicFormAssignment(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return deleteAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> headDynamicFormAssignment(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return headAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<Void> headFormInfo(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return headAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<FormInfoAspectResponseV2> getFormInfo(
      String urn, @jakarta.validation.Valid Boolean systemMetadata) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return getAspect(
        urn,
        systemMetadata,
        methodNameToAspectName(methodName),
        _respClazz,
        FormInfoAspectResponseV2.class);
  }

  public ResponseEntity<DynamicFormAssignmentAspectResponseV2> getDynamicFormAssignment(
      String urn, @jakarta.validation.Valid Boolean systemMetadata) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return getAspect(
        urn,
        systemMetadata,
        methodNameToAspectName(methodName),
        _respClazz,
        DynamicFormAssignmentAspectResponseV2.class);
  }

  public ResponseEntity<Void> deleteFormInfo(String urn) {
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return deleteAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<BusinessAttributeInfoAspectResponseV2> createBusinessAttributeInfo(
      BusinessAttributeInfoAspectRequestV2 body,
      String urn,
      @Nullable Boolean createIfNotExists,
      @Nullable Boolean createEntityIfNotExists) {
    if (checkBusinessAttributeFlagFromUrn(urn)) {
      throw new UnsupportedOperationException(BUSINESS_ATTRIBUTE_ERROR_MESSAGE);
    }
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return createAspect(
        urn,
        methodNameToAspectName(methodName),
        body,
        BusinessAttributeInfoAspectRequestV2.class,
        BusinessAttributeInfoAspectResponseV2.class,
        createIfNotExists,
        createEntityIfNotExists);
  }

  public ResponseEntity<Void> deleteBusinessAttributeInfo(String urn) {
    if (checkBusinessAttributeFlagFromUrn(urn)) {
      throw new UnsupportedOperationException(BUSINESS_ATTRIBUTE_ERROR_MESSAGE);
    }
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return deleteAspect(urn, methodNameToAspectName(methodName));
  }

  public ResponseEntity<BusinessAttributeInfoAspectResponseV2> getBusinessAttributeInfo(
      String urn, Boolean systemMetadata) {
    if (checkBusinessAttributeFlagFromUrn(urn)) {
      throw new UnsupportedOperationException(BUSINESS_ATTRIBUTE_ERROR_MESSAGE);
    }
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return getAspect(
        urn,
        systemMetadata,
        methodNameToAspectName(methodName),
        _respClazz,
        BusinessAttributeInfoAspectResponseV2.class);
  }

  public ResponseEntity<Void> headBusinessAttributeInfo(String urn) {
    if (checkBusinessAttributeFlagFromUrn(urn)) {
      throw new UnsupportedOperationException(BUSINESS_ATTRIBUTE_ERROR_MESSAGE);
    }
    String methodName =
        walker.walk(frames -> frames.findFirst().map(StackWalker.StackFrame::getMethodName)).get();
    return headAspect(urn, methodNameToAspectName(methodName));
  }

  private boolean checkBusinessAttributeFlagFromUrn(String urn) {
    try {
      return checkBusinessAttributeFlagFromEntityType(Urn.createFromString(urn).getEntityType());
    } catch (URISyntaxException e) {
      return true;
    }
  }

  private boolean checkBusinessAttributeFlagFromEntityType(String entityType) {
    return entityType.equals("businessAttribute") && !businessAttributeEntityEnabled();
  }

  private boolean businessAttributeEntityEnabled() {
    return System.getenv("BUSINESS_ATTRIBUTE_ENTITY_ENABLED") != null
        && Boolean.parseBoolean(System.getenv("BUSINESS_ATTRIBUTE_ENTITY_ENABLED"));
  }
}

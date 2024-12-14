package io.datahubproject.openapi.scim.repositories;

import static com.linkedin.metadata.Constants.*;
import static org.apache.directory.scim.core.repository.DefaultPatchHandler.*;

import com.datahub.util.RecordUtils;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Origin;
import com.linkedin.common.OriginType;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.directory.scim.core.repository.PatchHandler;
import org.apache.directory.scim.core.repository.Repository;
import org.apache.directory.scim.core.schema.SchemaRegistry;
import org.apache.directory.scim.server.exception.UnableToCreateResourceException;
import org.apache.directory.scim.server.exception.UnableToResolveIdResourceException;
import org.apache.directory.scim.spec.exception.ConflictResourceException;
import org.apache.directory.scim.spec.exception.ResourceException;
import org.apache.directory.scim.spec.filter.AttributeComparisonExpression;
import org.apache.directory.scim.spec.filter.AttributePresentExpression;
import org.apache.directory.scim.spec.filter.BaseFilterExpressionMapper;
import org.apache.directory.scim.spec.filter.CompareOperator;
import org.apache.directory.scim.spec.filter.Filter;
import org.apache.directory.scim.spec.filter.FilterResponse;
import org.apache.directory.scim.spec.filter.LogicalOperator;
import org.apache.directory.scim.spec.filter.PageRequest;
import org.apache.directory.scim.spec.filter.SortRequest;
import org.apache.directory.scim.spec.filter.ValuePathExpression;
import org.apache.directory.scim.spec.filter.attribute.AttributeReference;
import org.apache.directory.scim.spec.patch.PatchOperation;
import org.apache.directory.scim.spec.resources.ScimResource;
import org.apache.directory.scim.spec.schema.AttributeContainer;
import org.apache.directory.scim.spec.schema.Meta;
import org.apache.directory.scim.spec.schema.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Base class that handles abstract workflow of mapping between a ScimResource and a DataHub entity.
 * Also handles mapping of common SCIM attributes such as id, meta.
 *
 * @param <T> type of resource (ScimUser/ScimGroup)
 * @param <U> type of datahub urn (CorpUserUrn/CorpGroupUrn)
 * @param <K> type of datahub key (CorpUserKey/CorpGroupKey)
 */
@Slf4j
abstract class AbstractScimRepository<
        T extends ScimResource, U extends Urn, K extends RecordTemplate>
    implements Repository<T> {

  private static final String SCIM_CLIENT_PREFIX = "SCIM_client_";

  static String USERS_ENDPOINT = "/Users";

  static String GROUPS_ENDPOINT = "/Groups";

  @Autowired EntityService<?> _entityService;

  @Autowired
  @Qualifier("systemOperationContext")
  OperationContext systemOperationContext;

  @Autowired PatchHandler _patchHandler;

  @Autowired SchemaRegistry _schemaRegistry;

  /*
  Identifier mapping functions
   */

  /**
   * @param urnStr string representation of an urn
   * @return Urn object
   * @throws UnableToResolveIdResourceException
   */
  abstract U urnFromStr(String urnStr) throws UnableToResolveIdResourceException;

  /**
   * @param name logical id of the resource
   * @return Urn object; assumes the urn consists of only a "name" field
   */
  abstract U urnFromName(String name);

  /**
   * @param resource SCIM resource object
   * @return the name that is a logical id in DataHub
   */
  abstract String nameFromResource(T resource);

  /**
   * @param urn Urn object
   * @return SCIM resource id corresponding to the urn
   */
  /*
  Mapping from SCIM resource id to DataHub id is:
  SCIM resource id <==> Base64-url-encoded-urn

  Base64 encoding is chosen as a simplistic adherence to https://datatracker.ietf.org/doc/html/rfc7644#section-7.1 for
  usernames, especially ones that could be email addresses.
  For consistency, we're using the same mapping generally as well.
   */
  String urnToId(Urn urn) {
    return urnToId(urn.toString());
  }

  /**
   * @param urn Urn string
   * @return SCIM resource id corresponding to the urn
   */
  String urnToId(String urn) {
    return Base64.getUrlEncoder().encodeToString(urn.getBytes());
  }

  /**
   * @param id SCIM resource id
   * @return DataHub urn
   * @throws UnableToResolveIdResourceException
   */
  U urnFromId(String id) throws UnableToResolveIdResourceException {
    String urnStr = new String(Base64.getUrlDecoder().decode(id));
    return urnFromStr(urnStr);
  }

  // Extract name from a resource object, and map it to a DataHub urn.
  private U urnFromResource(T resource) throws UnsupportedEncodingException {
    String name = nameFromResource(resource);
    return urnFromName(name);
  }

  /*
  Error handling functions
   */
  abstract String invalidResourceMsg(String id);

  final UnableToResolveIdResourceException invalidResourceException(String id) {
    return new UnableToResolveIdResourceException(HttpStatus.NOT_FOUND, invalidResourceMsg(id));
  }

  abstract String resourceNotFoundMsg(String id);

  final UnableToResolveIdResourceException resourceNotFoundException(String id) {
    return new UnableToResolveIdResourceException(HttpStatus.NOT_FOUND, resourceNotFoundMsg(id));
  }

  /**
   * @return the DataHub entity corresponding to the SCIM resource (type parameter T)
   */
  abstract String entityName();

  private AuditStamp createAuditStamp() {
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(new CorpuserUrn("SCIM_client"), SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    return auditStamp;
  }

  /*
  CRUD functions.
  get(), delete(), create(), update(), patch()
   */
  @Override
  public final T get(String id) throws ResourceException {
    log.info("GET " + id);

    ScimReadableCorpEntity corpEntity = new ScimReadableCorpEntity(id);
    log.debug(String.format("GET-fetched entity %s with scim-id %s", corpEntity.urn, id));

    return corpEntity.asScimResource();
  }

  /*
  delete() functionality
   */
  void preDelete(U urn, AuditStamp auditStamp) {}

  @Override
  public final void delete(String id) throws ResourceException {
    AuditStamp auditStamp = createAuditStamp();
    U urn = urnFromId(id);
    preDelete(urn, auditStamp);
    if (!_entityService.exists(systemOperationContext, urn, false)) {
      throw resourceNotFoundException(id);
    }
    _entityService.deleteUrn(systemOperationContext, urn);

    log.debug(String.format("Deleted entity %s with scim-id %s", urn, id));

    // didn't seem to work
    // _deleteEntityService.deleteReferencesTo(urn, false);
  }

  /**
   * Ingest information from a SCIM resource into DataHub aspects
   *
   * @param scimResource the SCIM resource
   * @param urn urn of DataHub entity
   * @param auditStamp
   * @return DataHub aspects created
   */
  abstract Map<Class<? extends RecordTemplate>, RecordTemplate> ingestCreate(
      T scimResource, U urn, AuditStamp auditStamp);

  // utility function
  final void ingest(Urn urn, String aspectName, RecordTemplate aspect, AuditStamp auditStamp) {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(entityName());
    mcp.setAspectName(aspectName);
    mcp.setAspect(GenericRecordUtils.serializeAspect(aspect));
    mcp.setChangeType(ChangeType.UPSERT);
    _entityService.ingestProposal(systemOperationContext, mcp, auditStamp, false);
  }

  /*
  create() functionality
   */
  private Origin ingestOrigin(T resource, U urn, AuditStamp auditStamp) {
    Origin origin = new Origin();
    origin.setType(OriginType.EXTERNAL);
    origin.setExternalType(SCIM_CLIENT_PREFIX + resource.getExternalId());
    ingest(urn, ORIGIN_ASPECT_NAME, origin, auditStamp);
    return origin;
  }

  private String checkExternalIdIngestOrigin(
      T resource, AuditStamp auditStamp, ScimReadableCorpEntity corpEntity) {
    String externalId = corpEntity.getExternalId();
    if (!Objects.equals(resource.getExternalId(), corpEntity.getExternalId())) {
      ingestOrigin(resource, corpEntity.urn, auditStamp);
      externalId = resource.getExternalId();
    }
    return externalId;
  }

  private K getKey(Urn urn) {
    String keyAspectName = systemOperationContext.getKeyAspectName(urn);
    Set<String> aspectNames = ImmutableSet.of(systemOperationContext.getKeyAspectName(urn));
    return (K)
        _entityService
            .getLatestAspectsForUrn(systemOperationContext, urn, aspectNames, false)
            .get(keyAspectName);
  }

  @Override
  public final T create(T scimResource) throws ResourceException {
    log.info("CREATE " + scimResource);

    U urn = null;
    try {
      urn = urnFromResource(scimResource);
    } catch (UnsupportedEncodingException e) {
      throw new UnableToCreateResourceException(
          HttpStatus.BAD_REQUEST,
          "Resource name " + nameFromResource(scimResource) + " is invalid.");
    }

    if (_entityService.exists(systemOperationContext, urn, false)) {
      throw new ConflictResourceException(
          "Resource with name " + nameFromResource(scimResource) + " already exists.");
    }

    AuditStamp auditStamp = createAuditStamp();
    Origin origin = ingestOrigin(scimResource, urn, auditStamp);

    Map<Class<? extends RecordTemplate>, RecordTemplate> aspects =
        ingestCreate(scimResource, urn, auditStamp);
    aspects.put(Origin.class, origin);

    ScimReadableCorpEntity result =
        new ScimReadableCorpEntity(
            urn, getKey(urn), auditStamp.getTime(), auditStamp.getTime(), aspects);

    T resource = result.asScimResource();
    log.debug(String.format("Created entity %s with scim-id %s", urn, resource.getId()));

    return resource;
  }

  /*
   modification functionality, update() & patch()
  */

  /**
   * Check if key mutation is requested
   *
   * @param scimResource resource object containing requested updates
   * @param corpEntity existing DataHub entity information
   * @throws ResourceException if key mutation request is detected
   */
  abstract void checkKeyMutation(T scimResource, ScimReadableCorpEntity corpEntity)
      throws ResourceException;

  /**
   * Ingest any updates as appropriate.
   *
   * @param newResource the resource object containing the expected state of certain fields
   * @param corpEntity the existing DataHub entity information
   * @param auditStamp
   * @return the updated SCIM resource object
   */
  abstract T ingestUpdates(T newResource, ScimReadableCorpEntity corpEntity, AuditStamp auditStamp);

  private T update(T scimResource, ScimReadableCorpEntity corpEntity) {
    AuditStamp auditStamp = createAuditStamp();
    String externalId = checkExternalIdIngestOrigin(scimResource, auditStamp, corpEntity);

    T result = ingestUpdates(scimResource, corpEntity, auditStamp);
    result.setExternalId(externalId);

    return result;
  }

  @Override
  public final T update(
      String id,
      String version,
      T scimResource,
      Set<AttributeReference> includedAttributes,
      Set<AttributeReference> excludedAttributes)
      throws ResourceException {

    log.info("PUT " + id + " " + scimResource);

    ScimReadableCorpEntity corpEntity = new ScimReadableCorpEntity(id);
    checkKeyMutation(scimResource, corpEntity);

    T result = update(scimResource, corpEntity);

    log.debug(String.format("Updated entity %s with scim-id %s", corpEntity.urn, id));
    return result;
  }

  @Override
  public final T patch(
      String id,
      String version,
      List<PatchOperation> patchOperations,
      Set<AttributeReference> includedAttributes,
      Set<AttributeReference> excludedAttributes)
      throws ResourceException {

    log.info("PATCH " + id + " " + patchOperations);

    ScimReadableCorpEntity corpEntity = new ScimReadableCorpEntity(id);
    T existingResource = corpEntity.asScimResource();

    tweakBooleanPathExpressions(patchOperations, existingResource);

    T newResource = _patchHandler.apply(existingResource, patchOperations);

    T result = update(newResource, corpEntity);

    log.debug(String.format("Patched entity %s with scim-id %s", corpEntity.urn, id));
    return result;
  }

  /*
  This is to handle special cases where an IAM client sends a string value to be compared with a boolean attribute.
  For example, MS Entra ID patches roles with:
     PatchOperation(operation=ADD, path=roles[primary EQ "True"].value, value=Editor)
  Note the "True" string value for a boolean field.

  In this method, we modify the path in-place to replace the string ("True") with a boolean (true).

  Note that this implementation only checks for such occurrences at the root level of the path expression;
  to be completely defensive, it would need to walk the entire parse tree and defend against these occurrences.

  At the time of implementation, this complexity seems unwarranted for what is possibly just an anomaly in MS Entra ID.
  So we are only checking the root-level of the path expression.
  */
  private void tweakBooleanPathExpressions(
      List<PatchOperation> patchOperations, T existingResource) {
    patchOperations.forEach(
        patchOperation -> {
          ValuePathExpression valuePathExpression = valuePathExpression(patchOperation);
          if (valuePathExpression.getAttributeExpression()
              instanceof AttributeComparisonExpression comparisonExpression) {
            AttributeReference attributeReference = comparisonExpression.getAttributePath();
            Schema schema = _schemaRegistry.getSchema(existingResource.getBaseUrn());
            Schema.Attribute attribute =
                schema.getAttributeFromPath(attributeReference.getFullAttributeName());
            if (attribute.getType() == Schema.Attribute.Type.BOOLEAN
                && comparisonExpression.getCompareValue() instanceof String) {

              AttributeComparisonExpression updatedExpr =
                  new AttributeComparisonExpression(
                      attributeReference,
                      comparisonExpression.getOperation(),
                      Boolean.valueOf(comparisonExpression.getCompareValue().toString()));

              log.debug(
                  String.format(
                      "Tweaking PATCH path for boolean attribute from (%s) to (%s)",
                      comparisonExpression.toFilter(), updatedExpr.toFilter()));
              valuePathExpression.setAttributeExpression(updatedExpr);
            }
          }
        });
  }

  /*
  Functions used in flow of populating a SCIM resource object
   */

  /**
   * @return the type of SCIM resource
   */
  abstract String getResourceType();

  /**
   * @return aspects of the DataHub entity that are mapped to a SCIM resource object. In the
   *     returned map, key is aspect-name and value is the Class of the aspect.
   */
  abstract Map<String, Class<? extends RecordTemplate>> aspectNamesToClasses();

  /**
   * @return aspects whose modifications that are not to be considered as an update to the SCIM
   *     resource
   */
  Set<String> ignoreAspectsModificationTime() {
    return Collections.emptySet();
  }

  /**
   * @return a newly created, blank, SCIM resource object
   */
  abstract T newScimResource();

  /**
   * Populate relevant information from the DataHub entity-key into SCIM resource.
   *
   * @param key DataHub key
   * @param resource the SCIM resource object to be populated
   */
  abstract void sinkKeyToResource(K key, T resource);

  /**
   * Populate relevant information from the DataHub aspects into SCIM resource.
   *
   * @param urn
   * @param aspects aspects where key is aspect-class and value is aspect-object
   * @param resource the SCIM resource object to be populated
   */
  abstract void sinkAspectsToResource(
      U urn, Map<Class<? extends RecordTemplate>, RecordTemplate> aspects, T resource);

  /**
   * @return the SCIM endpoint
   */
  abstract String endpointName();

  private final String location(String id) {
    return location(endpointName(), id);
  }

  /*
  Create the meta.location value of a SCIM resource.
   */
  final String location(String endpoint, String id) {
    String MARKER = "/scim/v2";
    UriComponentsBuilder uriBuilder =
        ServletUriComponentsBuilder.fromCurrentRequestUri().replaceQuery(null);
    String uri = uriBuilder.build(true).toUri().toString();
    int index = uri.indexOf(MARKER);

    String result = uri.substring(0, index) + MARKER + endpoint + "/" + id;

    return result;
  }

  private static Map<Urn, List<EnvelopedAspect>> getAspectsIncludeOrigin(
      EntityService entityService,
      OperationContext operationContext,
      Set<Urn> urns,
      Set<String> aspectNames)
      throws UnableToResolveIdResourceException {
    if (!aspectNames.contains(ORIGIN_ASPECT_NAME)) {
      aspectNames = new HashSet<>(aspectNames);
      aspectNames.add(ORIGIN_ASPECT_NAME);
    }
    try {
      return entityService.getLatestEnvelopedAspects(operationContext, urns, aspectNames);
    } catch (URISyntaxException e) {
      throw new UnableToResolveIdResourceException(
          HttpStatus.NOT_FOUND, "Some urns caused URISyntaxException");
    }
  }

  /**
   * Class used for mapping a DataHub entity to a SCIM resource. Object-state contains information
   * derived from entityService. Transformation function {@link #asScimResource()} is used to create
   * a corresponding SCIM resource object.
   */
  @AllArgsConstructor
  class ScimReadableCorpEntity {
    @Getter private final U urn;

    @Getter private K key;

    @Getter private Long created;
    @Getter private Long lastModified;

    @Getter private final Map<Class<? extends RecordTemplate>, RecordTemplate> aspects;

    ScimReadableCorpEntity(String id) throws UnableToResolveIdResourceException {
      this(urnFromId(id));
    }

    ScimReadableCorpEntity(U urn) throws UnableToResolveIdResourceException {
      this(
          urn,
          getAspectsIncludeOrigin(
                  _entityService,
                  systemOperationContext,
                  ImmutableSet.of(urn),
                  aspectNamesToClasses().keySet())
              .get(urn));
    }

    ScimReadableCorpEntity(U urn, List<EnvelopedAspect> envAspects)
        throws UnableToResolveIdResourceException {
      if (envAspects == null) {
        throw resourceNotFoundException(urnToId(urn));
      }
      this.urn = urn;
      Map<String, Class<? extends RecordTemplate>> aspectNamesToClasses = aspectNamesToClasses();
      aspectNamesToClasses.putIfAbsent(ORIGIN_ASPECT_NAME, Origin.class);

      long lastModifiedAt = -1;

      this.aspects = new HashMap<>();

      Set<String> ignoreAspectsModificationTime = ignoreAspectsModificationTime();

      for (EnvelopedAspect envelopedAspect : envAspects) {
        String aspectName = envelopedAspect.getName();
        boolean isKeyAspect = systemOperationContext.getKeyAspectName(urn).equals(aspectName);

        Class<? extends RecordTemplate> aspectClass = aspectNamesToClasses.get(aspectName);
        RecordTemplate aspect =
            RecordUtils.toRecordTemplate(aspectClass, envelopedAspect.getValue().data());

        if (isKeyAspect) {
          if (envelopedAspect.getSystemMetadata(GetMode.NULL) == null) {
            // key aspect is somehow present for deleted entities as well without system metadata.
            throw resourceNotFoundException(urnToId(urn));
          }
          this.created = envelopedAspect.getCreated().getTime(GetMode.NULL);
          this.key = (K) aspect;
        } else {
          this.aspects.put(aspectClass, aspect);
        }
        if (!ignoreAspectsModificationTime.contains(aspectName)) {
          Long modifiedAt = envelopedAspect.getSystemMetadata().getLastObserved();
          if (modifiedAt != null && modifiedAt > lastModifiedAt) {
            lastModifiedAt = modifiedAt;
          }
        }
      }

      if (this.key == null) {
        throw resourceNotFoundException(urnToId(urn));
      }
      this.lastModified = lastModifiedAt;
    }

    <A extends RecordTemplate> A getAspect(Class<A> aspectClass) {
      return (A) aspects.get(aspectClass);
    }

    T asScimResource() {
      String id = urnToId(urn);
      T result = newScimResource();
      result.setId(id);

      var meta = new Meta();
      result.setMeta(meta);

      meta.setResourceType(getResourceType());
      meta.setLocation(location(id));
      if (created != null) {
        meta.setCreated(metaTimestamp(created));
      }
      if (lastModified != null) {
        meta.setLastModified(metaTimestamp(lastModified));
      }

      result.setExternalId(getExternalId());

      sinkKeyToResource(key, result);
      sinkAspectsToResource(urn, aspects, result);
      return result;
    }

    private String getExternalId() {
      Origin origin = (Origin) aspects.get(Origin.class);
      if (origin == null) {
        return null;
      }
      String externalId = origin.getExternalType().substring(SCIM_CLIENT_PREFIX.length());

      return externalId.equals("null") ? null : externalId;
    }
  }

  /*
  Query/find function.
  At this stage, we only support minimal functionality based on what's needed for MS Entra and Okta.
  That is, find users by userName, groups by groupName, and for Okta, pagination without filter.
   */

  // NOTE: this function isn't abstractable long-term when we want to support arbitrary filters.
  abstract String attributeNameToFindBy();

  @Override
  public final FilterResponse<T> find(
      Filter filter, PageRequest pageRequest, SortRequest sortRequest) throws ResourceException {

    List<T> result = new ArrayList<>();
    int totalResults;

    // Note: the current implementation simply ignores sortRequest
    if (filter == null) {
      if (pageRequest.getStartIndex() == null || pageRequest.getCount() == null) {
        throw new ResourceException(
            HttpStatus.BAD_REQUEST.value(), "filter and pageRequest parameters are both null.");
      }
      ListUrnsResult urns =
          _entityService.listUrns(
              systemOperationContext,
              entityName(),
              pageRequest.getStartIndex() - 1,
              pageRequest.getCount());

      Map<Urn, List<EnvelopedAspect>> aspects =
          getAspectsIncludeOrigin(
              _entityService,
              systemOperationContext,
              ImmutableSet.copyOf(urns.getEntities()),
              aspectNamesToClasses().keySet());

      for (Map.Entry<Urn, List<EnvelopedAspect>> entry : aspects.entrySet()) {
        ScimReadableCorpEntity scimEntity =
            new ScimReadableCorpEntity(urnFromStr(entry.getKey().toString()), entry.getValue());
        result.add(scimEntity.asScimResource());
      }

      totalResults =
          _entityService.getCountAspect(
              systemOperationContext, keyAspectName(), "urn:li:" + entityName() + ":%");
    } else {
      // note that pageRequest is ignored in this case
      FilterExpressionMapper filterMapper = new FilterExpressionMapper();
      String userName = filterMapper.apply(filter.getExpression(), null);

      if (Strings.isNullOrEmpty(userName)) {
        throw new ResourceException(HttpStatus.BAD_REQUEST.value(), "Unparsable filter: " + filter);
      }

      String id = urnToId(urnFromName(userName));
      try {
        result.add(get(id));
        totalResults = 1;
        log.debug(String.format("Found entity %s with scim-id %s", urnToId(id), id));
      } catch (UnableToResolveIdResourceException e) {
        // gulp, it just means no results match filter
        totalResults = 0;
      }
    }
    return new FilterResponse<>(result, pageRequest, totalResults);
  }

  protected abstract String keyAspectName();

  /*
  Class that walks the filter expression tree, and simply returns <value> if it
  finds an expression of the form <name-field> EQ <value>.
  If it does not find such an expression, it returns an empty string indicating that no objects match the filter.

  Other kinds of expressions are handled so that we err on the side of not returning any object. That is,
  if other nodes in the tree might possibly violate the above constraint, we return an empty string.
   */
  private class FilterExpressionMapper extends BaseFilterExpressionMapper<String> {

    @Override
    protected String apply(
        AttributeComparisonExpression attributeComparisonExpression,
        AttributeContainer attributeContainer) {
      /*
      attribute-names are case-insensitive per spec
      https://datatracker.ietf.org/doc/html/rfc7644#section-3.4.2.2
      */

      /*
       NOTE: we assume attribute-value will be of the exact case though the spec says otherwise
       (e.g. User.userName and Group.displayName both have caseExact = false
       as per https://datatracker.ietf.org/doc/html/rfc7643#section-2.2)
      */
      if (attributeComparisonExpression
              .getAttributePath()
              .getAttributeName()
              .equalsIgnoreCase(attributeNameToFindBy())
          && attributeComparisonExpression.getOperation().equals(CompareOperator.EQ)) {
        return attributeComparisonExpression.getCompareValue().toString();
      }
      return "";
    }

    @Override
    protected String apply(LogicalOperator logicalOperator, String left, String right) {
      // if we found something relevant lower in tree, bubble it up as-is
      if (!Strings.isNullOrEmpty(left) && logicalOperator.equals(LogicalOperator.OR)) {
        return left;
      }
      if (!Strings.isNullOrEmpty(right) && logicalOperator.equals(LogicalOperator.OR)) {
        return right;
      }
      return "";
    }

    @Override
    protected String negate(String s) {
      return "";
    }

    @Override
    protected String apply(
        AttributePresentExpression attributePresentExpression,
        AttributeContainer attributeContainer) {
      return "";
    }

    @Override
    protected String apply(
        ValuePathExpression valuePathExpression, AttributeContainer attributeContainer) {
      return "";
    }
  }

  /*
  Misc. utilities
  */
  static <A extends RecordTemplate> A getAspect(
      Map<Class<? extends RecordTemplate>, RecordTemplate> aspects, Class<A> aspectClass) {
    return (A) aspects.get(aspectClass);
  }

  static LocalDateTime metaTimestamp(long epochMilli) {
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), ZoneId.systemDefault());
  }

  static <E> List<E> listDiff(List<E> list1, List<E> list2) {
    Set<E> set1 = new LinkedHashSet<>(list1);
    set1.removeAll(list2);
    return set1.stream().toList();
  }
}

package io.datahubproject.iceberg.catalog;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.GenericRecordUtils.serializeAspect;
import static io.datahubproject.iceberg.catalog.Utils.*;

import com.google.common.base.Joiner;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.Container;
import com.linkedin.container.ContainerProperties;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.iceberg.catalog.credentials.CredentialProvider;
import io.datahubproject.iceberg.catalog.credentials.S3CredentialProvider;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.*;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.view.BaseMetastoreViewCatalog;
import org.apache.iceberg.view.ViewOperations;

public class DataHubRestCatalog extends BaseMetastoreViewCatalog implements SupportsNamespaces {
  private final CredentialProvider credentialProvider;

  static final int PAGE_SIZE = 100;

  // Upper bound for results on list namespaces/tables/views. Must use pagination for anything more
  // than that.
  static final int MAX_LIST_SIZE = 1000;

  private final EntityService entityService;

  private final EntitySearchService searchService;

  private final OperationContext operationContext;

  private final CloseableGroup closeableGroup;

  private final DataHubIcebergWarehouse warehouse;

  private final String warehouseRoot;

  private static final String CONTAINER_SUB_TYPE = "Namespace";

  public DataHubRestCatalog(
      EntityService entityService,
      EntitySearchService searchService,
      OperationContext operationContext,
      DataHubIcebergWarehouse warehouse,
      CredentialProvider credentialProvider) {
    this.entityService = entityService;
    this.searchService = searchService;
    this.operationContext = operationContext;
    this.credentialProvider = credentialProvider;
    this.warehouse = warehouse;

    if (warehouse.getDataRoot().endsWith("/")) {
      this.warehouseRoot = warehouse.getDataRoot();
    } else {
      this.warehouseRoot = warehouse.getDataRoot() + "/";
    }

    this.closeableGroup = new CloseableGroup();
    this.closeableGroup.setSuppressCloseFailure(true);
  }

  @Override
  public void renameView(TableIdentifier fromTableId, TableIdentifier toTableId) {
    renameTableOrView(fromTableId, toTableId, true);
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {}

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new DataHubTableOps(
        warehouse, tableIdentifier, entityService, operationContext, new S3FileIOFactory());
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    return warehouseRoot + tableIdentifier.toString().replaceAll("\\.", "/");
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return listTablesOrViews(namespace, "Table");
  }

  @Override
  public boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
    if (purge) {
      throw new UnsupportedOperationException();
    }

    return warehouse.deleteDataset(tableIdentifier);
  }

  @Override
  public Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
    if (tableExists(identifier)) {
      throw new AlreadyExistsException("Table already exists: %s", identifier);
    }

    FileIO io =
        new S3FileIOFactory()
            .createIO(
                platformInstance(),
                PoliciesConfig.DATA_READ_ONLY_PRIVILEGE,
                Set.of(parentDir(metadataFileLocation)));
    InputFile metadataFile = io.newInputFile(metadataFileLocation);
    TableMetadata metadata = TableMetadataParser.read(io, metadataFile);

    TableOperations ops = newTableOps(identifier);
    ops.commit(null, metadata);

    return new BaseTable(ops, fullTableName(name(), identifier), metricsReporter());
  }

  @Override
  public void renameTable(TableIdentifier fromTableId, TableIdentifier toTableId) {
    renameTableOrView(fromTableId, toTableId, false);
  }

  private void renameTableOrView(
      TableIdentifier fromTableId, TableIdentifier toTableId, boolean view) {
    if (!fromTableId.namespace().equals(toTableId.namespace())) {
      // check target namespace exists
      if (!entityService.exists(
          operationContext, containerUrn(platformInstance(), toTableId.namespace()))) {
        throw new NoSuchNamespaceException("Namespace does not exist: " + toTableId.namespace());
      }
    }
    AuditStamp auditStamp = auditStamp();
    DatasetUrn datasetUrn = warehouse.renameDataset(fromTableId, toTableId, false, auditStamp);
    DatasetProperties datasetProperties = new DatasetProperties();
    datasetProperties.setName(toTableId.name());
    datasetProperties.setQualifiedName(fullTableName(platformInstance(), toTableId));

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityType(DATASET_ENTITY_NAME);
    mcp.setAspectName(DATASET_PROPERTIES_ASPECT_NAME);
    mcp.setEntityUrn(datasetUrn);
    mcp.setAspect(serializeAspect(datasetProperties));
    mcp.setChangeType(ChangeType.UPSERT);
    ingestMcp(mcp, auditStamp);

    if (!fromTableId.namespace().equals(toTableId.namespace())) {
      Container container = new Container();
      container.setContainer(containerUrn(platformInstance(), toTableId.namespace()));

      MetadataChangeProposal containerMcp = new MetadataChangeProposal();
      containerMcp.setEntityType(DATASET_ENTITY_NAME);
      containerMcp.setAspectName(CONTAINER_ASPECT_NAME);
      containerMcp.setEntityUrn(datasetUrn);
      containerMcp.setAspect(serializeAspect(container));
      containerMcp.setChangeType(ChangeType.UPSERT);
      StringMap headers =
          new StringMap(
              Collections.singletonMap(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true)));
      mcp.setHeaders(headers);
      containerMcp.setHeaders(headers);
      ingestMcp(containerMcp, auditStamp);
    }
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> properties) {
    AuditStamp auditStamp = auditStamp();
    Urn containerUrn = containerUrn(platformInstance(), namespace);

    int nLevels = namespace.length();
    if (nLevels > 1) {
      String[] parentLevels = Arrays.copyOfRange(namespace.levels(), 0, nLevels - 1);
      Urn parentContainerUrn = containerUrn(platformInstance(), parentLevels);
      if (!entityService.exists(operationContext, parentContainerUrn)) {
        throw new NoSuchNamespaceException(
            "Parent namespace %s does not exist in platformInstance-catalog %s",
            Joiner.on(".").join(parentLevels), platformInstance());
      }
      ingestContainerAspect(
          containerUrn,
          CONTAINER_ASPECT_NAME,
          new Container().setContainer(parentContainerUrn),
          auditStamp);
    }

    ingestContainerAspect(
        containerUrn,
        SUB_TYPES_ASPECT_NAME,
        new SubTypes().setTypeNames(new StringArray(CONTAINER_SUB_TYPE)),
        auditStamp);

    ingestContainerProperties(namespace, properties, auditStamp);

    MetadataChangeProposal platformInstanceMcp =
        platformInstanceMcp(platformInstance(), containerUrn, CONTAINER_ENTITY_NAME);
    ingestMcp(platformInstanceMcp, auditStamp);
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    Filter filter;
    if (namespace.isEmpty()) {
      Criterion noParentCriterion = CriterionUtils.buildCriterion("container", Condition.IS_NULL);
      Criterion subTypeCriterion =
          CriterionUtils.buildCriterion("typeNames", Condition.EQUAL, CONTAINER_SUB_TYPE);
      Criterion dataPlatformInstanceCriterion =
          CriterionUtils.buildCriterion(
              "platformInstance",
              Condition.EQUAL,
              platformInstanceUrn(platformInstance()).toString());
      filter =
          QueryUtils.getFilterFromCriteria(
              List.of(noParentCriterion, subTypeCriterion, dataPlatformInstanceCriterion));
    } else {
      filter =
          QueryUtils.newFilter(
              "container.keyword", containerUrn(platformInstance(), namespace).toString());
    }

    SearchResult searchResult = search(filter, CONTAINER_ENTITY_NAME);

    return searchResult.getEntities().stream()
        .map(
            x -> {
              String namespaceName = namespaceNameFromContainerUrn(x.getEntity());
              return Namespace.of(namespaceName.split("\\."));
            })
        .collect(Collectors.toList());
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {

    ContainerProperties containerProperties =
        (ContainerProperties)
            entityService.getLatestAspect(
                operationContext,
                containerUrn(platformInstance(), namespace),
                CONTAINER_PROPERTIES_ASPECT_NAME);

    if (containerProperties == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: " + namespace);
    }

    return new HashMap<>(containerProperties.getCustomProperties());
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    Urn containerUrn = containerUrn(platformInstance(), namespace);
    if (!entityService.exists(operationContext, containerUrn)) {
      return false;
    }

    Filter filter = QueryUtils.newFilter("container.keyword", containerUrn.toString());

    if (searchIsEmpty(filter, CONTAINER_ENTITY_NAME, DATASET_ENTITY_NAME)) {
      // TODO handle race conditions
      entityService.deleteUrn(operationContext, containerUrn);
      return true;
    } else {
      throw new NamespaceNotEmptyException("Namespace %s is not empty", namespace);
    }
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> map)
      throws NoSuchNamespaceException {
    // not required for our purposes currently
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> set)
      throws NoSuchNamespaceException {
    // not required for our purposes currently
    throw new UnsupportedOperationException();
  }

  private void ingestContainerProperties(
      Namespace namespace, Map<String, String> properties, AuditStamp auditStamp) {
    ingestContainerAspect(
        containerUrn(platformInstance(), namespace),
        CONTAINER_PROPERTIES_ASPECT_NAME,
        new ContainerProperties()
            .setName(namespace.levels()[namespace.length() - 1])
            .setCustomProperties(new StringMap(properties)),
        auditStamp);
  }

  public UpdateNamespacePropertiesResponse updateNamespaceProperties(
      Namespace namespace, UpdateNamespacePropertiesRequest request) {

    Map<String, String> properties = loadNamespaceMetadata(namespace);

    Set<String> missing = new HashSet<>();
    request.removals().stream()
        .filter(Predicate.not(properties::containsKey))
        .forEach(missing::add);

    UpdateNamespacePropertiesResponse.Builder responseBuilder =
        UpdateNamespacePropertiesResponse.builder();

    request.removals().stream()
        .filter(Predicate.not(missing::contains))
        .forEach(responseBuilder::addRemoved);

    responseBuilder.addUpdated(request.updates().keySet());
    responseBuilder.addMissing(missing);

    properties.putAll(request.updates());
    properties.keySet().removeAll(request.removals());

    ingestContainerProperties(namespace, properties, auditStamp());

    return responseBuilder.build();
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.closeableGroup.close();
  }

  private void ingestContainerAspect(
      Urn containerUrn, String aspectName, RecordTemplate aspect, AuditStamp auditStamp) {
    MetadataChangeProposal mcp = new MetadataChangeProposal();

    mcp.setEntityUrn(containerUrn);
    mcp.setEntityType(CONTAINER_ENTITY_NAME);
    mcp.setAspectName(aspectName);
    mcp.setAspect(serializeAspect(aspect));
    mcp.setChangeType(ChangeType.UPSERT);

    StringMap headers =
        new StringMap(
            Collections.singletonMap(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true)));
    mcp.setHeaders(headers);

    ingestMcp(mcp, auditStamp);
  }

  private void ingestMcp(MetadataChangeProposal mcp, AuditStamp auditStamp) {
    entityService.ingestProposal(operationContext, mcp, auditStamp, false);
  }

  private List<TableIdentifier> listTablesOrViews(Namespace namespace, String typeName) {
    Filter filter =
        QueryUtils.newFilter(
            Map.of(
                "container.keyword",
                containerUrn(platformInstance(), namespace).toString(),
                "typeNames",
                typeName));

    SearchResult searchResult = search(filter, DATASET_ENTITY_NAME);

    Set<Urn> urns =
        searchResult.getEntities().stream()
            .map(SearchEntity::getEntity)
            .collect(Collectors.toSet());

    Map<Urn, List<RecordTemplate>> aspects =
        entityService.getLatestAspects(
            operationContext, urns, Set.of(DATASET_PROPERTIES_ASPECT_NAME), false);

    return aspects.values().stream()
        .filter(x -> x != null && !x.isEmpty())
        .map(x -> (DatasetProperties) x.get(0))
        .map(DatasetProperties::getQualifiedName)
        .map(
            x -> {
              String[] parts = x.split("\\.");
              // ignore first part which is the warehouse name
              return TableIdentifier.of(Arrays.copyOfRange(parts, 1, parts.length));
            })
        .toList();
  }

  private SearchResult search(Filter filter, String entityName) {
    // Go through pages.
    SearchEntityArray allEntities = new SearchEntityArray();
    int startIndex = 0;
    int totalCount;

    do {
      SearchResult pageResult =
          searchService.search(
              operationContext, List.of(entityName), "*", filter, List.of(), startIndex, PAGE_SIZE);
      totalCount = pageResult.getNumEntities();
      if (totalCount > MAX_LIST_SIZE) {
        totalCount = MAX_LIST_SIZE;
      }
      allEntities.addAll(pageResult.getEntities());
      startIndex += PAGE_SIZE;
    } while (startIndex < totalCount);

    SearchResult allResults = new SearchResult();
    allResults.setEntities(allEntities);
    return allResults;
  }

  private boolean searchIsEmpty(Filter filter, String... entityNames) {
    SearchResult searchResult =
        searchService.search(operationContext, List.of(entityNames), "*", filter, List.of(), 0, 1);

    return searchResult.getEntities().isEmpty();
  }

  private class S3FileIOFactory implements FileIOFactory {
    @Override
    public FileIO createIO(
        String platformInstance, PoliciesConfig.Privilege privilege, Set<String> locations) {

      FileIO io = new S3FileIO();
      Map<String, String> creds =
          credentialProvider.getCredentials(
              new S3CredentialProvider.CredentialsCacheKey(platformInstance, privilege, locations),
              warehouse.getStorageProviderCredentials());
      io.initialize(creds);
      closeableGroup.addCloseable(io);
      return io;
    }

    @Override
    public FileIO createIO(
        String platformInstance, PoliciesConfig.Privilege privilege, TableMetadata tableMetadata) {
      return createIO(platformInstance, privilege, locations(tableMetadata));
    }
  }

  @Override
  protected ViewOperations newViewOps(TableIdentifier tableIdentifier) {
    return new DataHubViewOps(
        warehouse, tableIdentifier, entityService, operationContext, new S3FileIOFactory());
  }

  @Override
  public List<TableIdentifier> listViews(Namespace namespace) {
    return listTablesOrViews(namespace, "View");
  }

  @Override
  public boolean dropView(TableIdentifier tableIdentifier) {
    return warehouse.deleteDataset(tableIdentifier);
  }

  private String platformInstance() {
    return warehouse.getPlatformInstance();
  }
}

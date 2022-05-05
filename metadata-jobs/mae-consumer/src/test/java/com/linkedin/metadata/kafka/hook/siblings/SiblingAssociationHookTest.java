package com.linkedin.metadata.kafka.hook.siblings;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Siblings;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;


public class SiblingAssociationHookTest {
  private SiblingAssociationHook _siblingAssociationHook;
  EntityService _mockEntityService;
  SearchService _mockSearchService;

  @BeforeMethod
  public void setupTest() {
    EntityRegistry registry = new ConfigEntityRegistry(
        SiblingAssociationHookTest.class.getClassLoader().getResourceAsStream("test-entity-registry-siblings.yml"));
    _mockEntityService = Mockito.mock(EntityService.class);
    _mockSearchService = Mockito.mock(SearchService.class);
    _siblingAssociationHook = new SiblingAssociationHook(registry, _mockEntityService, _mockSearchService);
    _siblingAssociationHook.setEnabled(true);
  }

  @Test
  public void testInvokeWhenThereIsAPair() throws Exception {
    SearchResult returnSearchResult = new SearchResult();
    SearchEntityArray returnEntityArray = new SearchEntityArray();
    SearchEntity returnArrayValue = new SearchEntity();
    returnArrayValue.setEntity(
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)")
    );
    returnEntityArray.add(returnArrayValue);

    returnSearchResult.setEntities(returnEntityArray);

    Mockito.when(
        _mockSearchService.search(
            anyString(), anyString(), any(), any(), anyInt(), anyInt(), any()
        )).thenReturn(returnSearchResult);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(DATASET_PROPERTIES_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final DatasetProperties dbtProperties = new DatasetProperties();
    dbtProperties.setName("dbt display name");
    dbtProperties.setTags(new StringArray());

    event.setAspect(GenericRecordUtils.serializeAspect(dbtProperties));
    event.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.jaffle_shop.customers,PROD)"));
    _siblingAssociationHook.invoke(event);

    final Siblings dbtSiblingsAspect = new Siblings()
        .setSiblings(new UrnArray(ImmutableList.of(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)"))))
        .setPrimary(true);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.jaffle_shop.customers,PROD)"));
    proposal.setEntityType(DATASET_ENTITY_NAME);
    proposal.setAspectName(SIBLINGS_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(dbtSiblingsAspect));
    proposal.setChangeType(ChangeType.UPSERT);

    Mockito.verify(_mockEntityService, Mockito.times(1)).ingestProposal(
        Mockito.eq(proposal),
        Mockito.any(AuditStamp.class)
    );

    final Siblings sourceSiblingsAspect = new Siblings()
        .setSiblings(new UrnArray(ImmutableList.of(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.jaffle_shop.customers,PROD)"))))
        .setPrimary(false);

    final MetadataChangeProposal proposal2 = new MetadataChangeProposal();
    proposal2.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)"));
    proposal2.setEntityType(DATASET_ENTITY_NAME);
    proposal2.setAspectName(SIBLINGS_ASPECT_NAME);
    proposal2.setAspect(GenericRecordUtils.serializeAspect(sourceSiblingsAspect));
    proposal2.setChangeType(ChangeType.UPSERT);

    Mockito.verify(_mockEntityService, Mockito.times(1)).ingestProposal(
        Mockito.eq(proposal2),
        Mockito.any(AuditStamp.class)
    );

    final Status sourceStatusAspect = new Status().setRemoved(true);

    final MetadataChangeProposal proposal3 = new MetadataChangeProposal();
    proposal3.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)"));
    proposal3.setEntityType(DATASET_ENTITY_NAME);
    proposal3.setAspectName(STATUS_ASPECT_NAME);
    proposal3.setAspect(GenericRecordUtils.serializeAspect(sourceStatusAspect));
    proposal3.setChangeType(ChangeType.UPSERT);

    Mockito.verify(_mockEntityService, Mockito.times(1)).ingestProposal(
        Mockito.eq(proposal3),
        Mockito.any(AuditStamp.class)
    );
  }

  @Test
  public void testInvokeWhenThereIsNoPair() throws Exception {
    SearchResult returnSearchResult = new SearchResult();
    SearchEntityArray returnEntityArray = new SearchEntityArray();

    returnSearchResult.setEntities(returnEntityArray);

    Mockito.when(
        _mockSearchService.search(
            anyString(), anyString(), any(), any(), anyInt(), anyInt(), any()
        )).thenReturn(returnSearchResult);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(DATASET_PROPERTIES_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final DatasetProperties dbtProperties = new DatasetProperties();
    dbtProperties.setName("dbt display name");
    dbtProperties.setTags(new StringArray());

    event.setAspect(GenericRecordUtils.serializeAspect(dbtProperties));
    event.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.jaffle_shop.customers,PROD)"));
    _siblingAssociationHook.invoke(event);

    final Siblings dbtSiblingsAspect = new Siblings()
        .setSiblings(new UrnArray(ImmutableList.of(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)"))))
        .setPrimary(true);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.jaffle_shop.customers,PROD)"));
    proposal.setEntityType(DATASET_ENTITY_NAME);
    proposal.setAspectName(SIBLINGS_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(dbtSiblingsAspect));
    proposal.setChangeType(ChangeType.UPSERT);

    Mockito.verify(_mockEntityService, Mockito.times(0)).ingestProposal(
        Mockito.eq(proposal),
        Mockito.any(AuditStamp.class)
    );

    final Siblings sourceSiblingsAspect = new Siblings()
        .setSiblings(new UrnArray(ImmutableList.of(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.jaffle_shop.customers,PROD)"))))
        .setPrimary(false);

    final MetadataChangeProposal proposal2 = new MetadataChangeProposal();
    proposal2.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)"));
    proposal2.setEntityType(DATASET_ENTITY_NAME);
    proposal2.setAspectName(SIBLINGS_ASPECT_NAME);
    proposal2.setAspect(GenericRecordUtils.serializeAspect(sourceSiblingsAspect));
    proposal2.setChangeType(ChangeType.UPSERT);

    Mockito.verify(_mockEntityService, Mockito.times(0)).ingestProposal(
        Mockito.eq(proposal2),
        Mockito.any(AuditStamp.class)
    );

    final Status sourceStatusAspect = new Status().setRemoved(true);

    final MetadataChangeProposal proposal3 = new MetadataChangeProposal();
    proposal3.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)"));
    proposal3.setEntityType(DATASET_ENTITY_NAME);
    proposal3.setAspectName(STATUS_ASPECT_NAME);
    proposal3.setAspect(GenericRecordUtils.serializeAspect(sourceStatusAspect));
    proposal3.setChangeType(ChangeType.UPSERT);

    Mockito.verify(_mockEntityService, Mockito.times(0)).ingestProposal(
        Mockito.eq(proposal3),
        Mockito.any(AuditStamp.class)
    );
  }
}

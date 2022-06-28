package com.linkedin.metadata.boot.steps;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.mxe.MetadataChangeProposal;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class RestoreGlossaryIndicesTest {

  private static final String VERSION_1 = "1";
  private static final String VERSION_2 = "2";
  private static final String GLOSSARY_UPGRADE_URN = String.format("urn:li:%s:%s", Constants.DATA_HUB_UPGRADE_ENTITY_NAME, "restore-glossary-indices-ui");

  private void mockGetTermInfo(Urn glossaryTermUrn, EntitySearchService mockSearchService, EntityService mockService) throws Exception {
    Map<String, EnvelopedAspect> termInfoAspects = new HashMap<>();
    termInfoAspects.put(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(new GlossaryTermInfo().setName("test").data())));
    Map<Urn, EntityResponse> termInfoResponses = new HashMap<>();
    termInfoResponses.put(glossaryTermUrn, new EntityResponse().setUrn(glossaryTermUrn).setAspects(new EnvelopedAspectMap(termInfoAspects)));
    Mockito.when(mockSearchService.search(Constants.GLOSSARY_TERM_ENTITY_NAME, "", null, null, 0, 1000))
        .thenReturn(new SearchResult().setNumEntities(1).setEntities(new SearchEntityArray(ImmutableList.of(new SearchEntity().setEntity(glossaryTermUrn)))));
    Mockito.when(mockService.getEntitiesV2(
            Constants.GLOSSARY_TERM_ENTITY_NAME,
            new HashSet<>(Collections.singleton(glossaryTermUrn)),
            Collections.singleton(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME)))
        .thenReturn(termInfoResponses);
  }

  private void mockGetNodeInfo(Urn glossaryNodeUrn, EntitySearchService mockSearchService, EntityService mockService) throws Exception {
    Map<String, EnvelopedAspect> nodeInfoAspects = new HashMap<>();
    nodeInfoAspects.put(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(new GlossaryNodeInfo().setName("test").data())));
    Map<Urn, EntityResponse> nodeInfoResponses = new HashMap<>();
    nodeInfoResponses.put(glossaryNodeUrn, new EntityResponse().setUrn(glossaryNodeUrn).setAspects(new EnvelopedAspectMap(nodeInfoAspects)));
    Mockito.when(mockSearchService.search(Constants.GLOSSARY_NODE_ENTITY_NAME, "", null, null, 0, 1000))
        .thenReturn(new SearchResult().setNumEntities(1).setEntities(new SearchEntityArray(ImmutableList.of(new SearchEntity().setEntity(glossaryNodeUrn)))));
    Mockito.when(mockService.getEntitiesV2(
            Constants.GLOSSARY_NODE_ENTITY_NAME,
            new HashSet<>(Collections.singleton(glossaryNodeUrn)),
            Collections.singleton(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME)
        ))
        .thenReturn(nodeInfoResponses);
  }

  private AspectSpec mockGlossaryAspectSpecs(EntityRegistry mockRegistry) {
    EntitySpec entitySpec = Mockito.mock(EntitySpec.class);
    AspectSpec aspectSpec = Mockito.mock(AspectSpec.class);
    //  Mock for Terms
    Mockito.when(mockRegistry.getEntitySpec(Constants.GLOSSARY_TERM_ENTITY_NAME)).thenReturn(entitySpec);
    Mockito.when(entitySpec.getAspectSpec(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME)).thenReturn(aspectSpec);
    //  Mock for Nodes
    Mockito.when(mockRegistry.getEntitySpec(Constants.GLOSSARY_NODE_ENTITY_NAME)).thenReturn(entitySpec);
    Mockito.when(entitySpec.getAspectSpec(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME)).thenReturn(aspectSpec);

    return aspectSpec;
  }

  @Test
  public void testExecuteFirstTime() throws Exception {
    final Urn glossaryTermUrn = Urn.createFromString("urn:li:glossaryTerm:11115397daf94708a8822b8106cfd451");
    final Urn glossaryNodeUrn = Urn.createFromString("urn:li:glossaryNode:22225397daf94708a8822b8106cfd451");
    final EntityService mockService = Mockito.mock(EntityService.class);
    final EntitySearchService mockSearchService = Mockito.mock(EntitySearchService.class);
    final EntityRegistry mockRegistry = Mockito.mock(EntityRegistry.class);

    final Urn upgradeEntityUrn = Urn.createFromString(GLOSSARY_UPGRADE_URN);
    Mockito.when(mockService.getEntityV2(
        Constants.DATA_HUB_UPGRADE_ENTITY_NAME,
        upgradeEntityUrn,
        Collections.singleton(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME)
    )).thenReturn(null);

    mockGetTermInfo(glossaryTermUrn, mockSearchService, mockService);
    mockGetNodeInfo(glossaryNodeUrn, mockSearchService, mockService);

    AspectSpec aspectSpec = mockGlossaryAspectSpecs(mockRegistry);

    RestoreGlossaryIndices restoreIndicesStep = new RestoreGlossaryIndices(mockService, mockSearchService, mockRegistry);
    restoreIndicesStep.execute();


    Mockito.verify(mockRegistry, Mockito.times(1)).getEntitySpec(Constants.GLOSSARY_TERM_ENTITY_NAME);
    Mockito.verify(mockRegistry, Mockito.times(1)).getEntitySpec(Constants.GLOSSARY_NODE_ENTITY_NAME);
    Mockito.verify(mockService, Mockito.times(2)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(AuditStamp.class)
        );
    Mockito.verify(mockService, Mockito.times(1)).produceMetadataChangeLog(
        Mockito.eq(glossaryTermUrn),
        Mockito.eq(Constants.GLOSSARY_TERM_ENTITY_NAME),
        Mockito.eq(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME),
        Mockito.eq(aspectSpec),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(null),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(ChangeType.RESTATE)
    );
    Mockito.verify(mockService, Mockito.times(1)).produceMetadataChangeLog(
        Mockito.eq(glossaryNodeUrn),
        Mockito.eq(Constants.GLOSSARY_NODE_ENTITY_NAME),
        Mockito.eq(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME),
        Mockito.eq(aspectSpec),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(null),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(ChangeType.RESTATE)
    );
  }

  @Test
  public void testExecutesWithNewVersion() throws Exception {
    final Urn glossaryTermUrn = Urn.createFromString("urn:li:glossaryTerm:11115397daf94708a8822b8106cfd451");
    final Urn glossaryNodeUrn = Urn.createFromString("urn:li:glossaryNode:22225397daf94708a8822b8106cfd451");
    final EntityService mockService = Mockito.mock(EntityService.class);
    final EntitySearchService mockSearchService = Mockito.mock(EntitySearchService.class);
    final EntityRegistry mockRegistry = Mockito.mock(EntityRegistry.class);

    final Urn upgradeEntityUrn = Urn.createFromString(GLOSSARY_UPGRADE_URN);
    com.linkedin.upgrade.DataHubUpgradeRequest upgradeRequest = new com.linkedin.upgrade.DataHubUpgradeRequest().setVersion(VERSION_2);
    Map<String, EnvelopedAspect> upgradeRequestAspects = new HashMap<>();
    upgradeRequestAspects.put(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(upgradeRequest.data())));
    EntityResponse response = new EntityResponse().setAspects(new EnvelopedAspectMap(upgradeRequestAspects));
    Mockito.when(mockService.getEntityV2(
        Constants.DATA_HUB_UPGRADE_ENTITY_NAME,
        upgradeEntityUrn,
        Collections.singleton(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME)
    )).thenReturn(response);

    mockGetTermInfo(glossaryTermUrn, mockSearchService, mockService);
    mockGetNodeInfo(glossaryNodeUrn, mockSearchService, mockService);

    AspectSpec aspectSpec = mockGlossaryAspectSpecs(mockRegistry);

    RestoreGlossaryIndices restoreIndicesStep = new RestoreGlossaryIndices(mockService, mockSearchService, mockRegistry);
    restoreIndicesStep.execute();


    Mockito.verify(mockRegistry, Mockito.times(1)).getEntitySpec(Constants.GLOSSARY_TERM_ENTITY_NAME);
    Mockito.verify(mockRegistry, Mockito.times(1)).getEntitySpec(Constants.GLOSSARY_NODE_ENTITY_NAME);
    Mockito.verify(mockService, Mockito.times(2)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(AuditStamp.class)
        );
    Mockito.verify(mockService, Mockito.times(1)).produceMetadataChangeLog(
        Mockito.eq(glossaryTermUrn),
        Mockito.eq(Constants.GLOSSARY_TERM_ENTITY_NAME),
        Mockito.eq(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME),
        Mockito.eq(aspectSpec),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(null),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(ChangeType.RESTATE)
    );
    Mockito.verify(mockService, Mockito.times(1)).produceMetadataChangeLog(
        Mockito.eq(glossaryNodeUrn),
        Mockito.eq(Constants.GLOSSARY_NODE_ENTITY_NAME),
        Mockito.eq(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME),
        Mockito.eq(aspectSpec),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(null),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(ChangeType.RESTATE)
    );
  }

  @Test
  public void testDoesNotRunWhenAlreadyExecuted() throws Exception {
    final Urn glossaryTermUrn = Urn.createFromString("urn:li:glossaryTerm:11115397daf94708a8822b8106cfd451");
    final Urn glossaryNodeUrn = Urn.createFromString("urn:li:glossaryNode:22225397daf94708a8822b8106cfd451");
    final EntityService mockService = Mockito.mock(EntityService.class);
    final EntitySearchService mockSearchService = Mockito.mock(EntitySearchService.class);
    final EntityRegistry mockRegistry = Mockito.mock(EntityRegistry.class);

    final Urn upgradeEntityUrn = Urn.createFromString(GLOSSARY_UPGRADE_URN);
    com.linkedin.upgrade.DataHubUpgradeRequest upgradeRequest = new com.linkedin.upgrade.DataHubUpgradeRequest().setVersion(VERSION_1);
    Map<String, EnvelopedAspect> upgradeRequestAspects = new HashMap<>();
    upgradeRequestAspects.put(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(upgradeRequest.data())));
    EntityResponse response = new EntityResponse().setAspects(new EnvelopedAspectMap(upgradeRequestAspects));
    Mockito.when(mockService.getEntityV2(
        Constants.DATA_HUB_UPGRADE_ENTITY_NAME,
        upgradeEntityUrn,
        Collections.singleton(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME)
    )).thenReturn(response);

    RestoreGlossaryIndices restoreIndicesStep = new RestoreGlossaryIndices(mockService, mockSearchService, mockRegistry);
    restoreIndicesStep.execute();

    Mockito.verify(mockRegistry, Mockito.times(0)).getEntitySpec(Constants.GLOSSARY_TERM_ENTITY_NAME);
    Mockito.verify(mockRegistry, Mockito.times(0)).getEntitySpec(Constants.GLOSSARY_NODE_ENTITY_NAME);
    Mockito.verify(mockSearchService, Mockito.times(0)).search(Constants.GLOSSARY_TERM_ENTITY_NAME, "", null, null, 0, 1000);
    Mockito.verify(mockSearchService, Mockito.times(0)).search(Constants.GLOSSARY_NODE_ENTITY_NAME, "", null, null, 0, 1000);
    Mockito.verify(mockService, Mockito.times(0)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(AuditStamp.class)
    );
    Mockito.verify(mockService, Mockito.times(0)).produceMetadataChangeLog(
        Mockito.eq(glossaryTermUrn),
        Mockito.eq(Constants.GLOSSARY_TERM_ENTITY_NAME),
        Mockito.eq(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME),
        Mockito.any(),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(null),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(ChangeType.RESTATE)
    );
    Mockito.verify(mockService, Mockito.times(0)).produceMetadataChangeLog(
        Mockito.eq(glossaryNodeUrn),
        Mockito.eq(Constants.GLOSSARY_NODE_ENTITY_NAME),
        Mockito.eq(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME),
        Mockito.any(),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(null),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(ChangeType.RESTATE)
    );
  }
}

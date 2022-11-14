package com.linkedin.datahub.graphql.types.notebook;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.ChangeAuditStamps;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.InstitutionalMemoryMetadataArray;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.NotebookUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.notebook.NotebookCell;
import com.linkedin.notebook.NotebookCellArray;
import com.linkedin.notebook.NotebookCellType;
import com.linkedin.notebook.NotebookContent;
import com.linkedin.notebook.NotebookInfo;
import com.linkedin.notebook.EditableNotebookProperties;
import com.linkedin.notebook.TextCell;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Notebook;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.container.ContainerType;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.NotebookKey;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class NotebookTypeTest {
  private static final String TEST_NOTEBOOK = "urn:li:notebook:(querybook,123)";
  private static final NotebookKey NOTEBOOK_KEY = new NotebookKey()
      .setNotebookId("123")
      .setNotebookTool("querybook");
  private static final NotebookContent NOTEBOOK_CONTENT = new NotebookContent()
      .setCells(new NotebookCellArray(ImmutableList.of(new NotebookCell()
          .setType(NotebookCellType.TEXT_CELL)
          .setTextCell(new TextCell()
              .setCellId("1234")
              .setCellTitle("test cell")
              .setText("test text")
              .setChangeAuditStamps(new ChangeAuditStamps())))));
  private static final EditableNotebookProperties TEST_EDITABLE_DESCRIPTION = new EditableNotebookProperties()
      .setDescription("test editable description");
  private static final Ownership OWNERSHIP = new Ownership()
      .setOwners(
          new OwnerArray(ImmutableList.of(
              new Owner()
                  .setType(OwnershipType.DATAOWNER)
                  .setOwner(Urn.createFromTuple("corpuser", "test")))));
  private static final InstitutionalMemory INSTITUTIONAL_MEMORY = new InstitutionalMemory()
      .setElements(
          new InstitutionalMemoryMetadataArray(ImmutableList.of(
              new InstitutionalMemoryMetadata()
                  .setUrl(new Url("https://www.test.com"))
                  .setDescription("test description")
                  .setCreateStamp(new AuditStamp().setTime(0L).setActor(Urn.createFromTuple("corpuser", "test"))))));

  private static final SubTypes SUB_TYPES = new SubTypes().setTypeNames(new StringArray(ImmutableList.of("DataDoc")));

  private static final DataPlatformInstance DATA_PLATFORM_INSTANCE = new DataPlatformInstance()
      .setPlatform(new DataPlatformUrn("test_platform"));

  private static final NotebookInfo NOTEBOOK_INFO = new NotebookInfo()
      .setTitle("title")
      .setExternalUrl(new Url("https://querybook.com/notebook/123"))
      .setChangeAuditStamps(new ChangeAuditStamps())
      .setDescription("test doc");

  private static final Status STATUS = new Status()
      .setRemoved(false);

  private static final Domains DOMAINS = new Domains()
      .setDomains(new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:domain:123"))));
  private static final GlobalTags GLOBAL_TAGS = new GlobalTags()
      .setTags(new TagAssociationArray(ImmutableList.of(new TagAssociation().setTag(new TagUrn("test")))));
  private static final GlossaryTerms TEST_GLOSSARY_TERMS = new GlossaryTerms()
      .setTerms(new GlossaryTermAssociationArray(ImmutableList.of(new GlossaryTermAssociation().setUrn(new GlossaryTermUrn("term")))));

  @Test
  public void testBatchLoad() throws Exception {

    EntityClient client = Mockito.mock(EntityClient.class);

    Map<String, EnvelopedAspect> notebookAspects = new HashMap<>();
    notebookAspects.put(
        Constants.NOTEBOOK_KEY_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(NOTEBOOK_KEY.data()))
    );
    notebookAspects.put(
        Constants.NOTEBOOK_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(NOTEBOOK_INFO.data()))
    );
    notebookAspects.put(
        Constants.NOTEBOOK_CONTENT_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(NOTEBOOK_CONTENT.data()))
    );
    notebookAspects.put(
        Constants.EDITABLE_NOTEBOOK_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_EDITABLE_DESCRIPTION.data()))
    );
    notebookAspects.put(
        Constants.OWNERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(OWNERSHIP.data()))
    );
    notebookAspects.put(
        Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(INSTITUTIONAL_MEMORY.data()))
    );
    notebookAspects.put(
        Constants.STATUS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(STATUS.data()))
    );
    notebookAspects.put(
        Constants.GLOBAL_TAGS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(GLOBAL_TAGS.data()))
    );
    notebookAspects.put(
        Constants.DOMAINS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(DOMAINS.data()))
    );
    notebookAspects.put(
        Constants.SUB_TYPES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(SUB_TYPES.data()))
    );
    notebookAspects.put(
        Constants.GLOSSARY_TERMS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_GLOSSARY_TERMS.data()))
    );
    notebookAspects.put(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(DATA_PLATFORM_INSTANCE.data())));

    Urn notebookUrn = new NotebookUrn("querybook", "123");
    Urn dummyNotebookUrn = new NotebookUrn("querybook", "dummy");
    Mockito.when(client.batchGetV2(
        Mockito.eq(Constants.NOTEBOOK_ENTITY_NAME),
        Mockito.eq(new HashSet<>(ImmutableSet.of(notebookUrn, dummyNotebookUrn))),
        Mockito.eq(NotebookType.ASPECTS_TO_RESOLVE),
        Mockito.any(Authentication.class)))
        .thenReturn(ImmutableMap.of(
            notebookUrn,
            new EntityResponse()
                .setEntityName(Constants.NOTEBOOK_ENTITY_NAME)
                .setUrn(notebookUrn)
                .setAspects(new EnvelopedAspectMap(notebookAspects))));

    NotebookType type = new NotebookType(client);

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    List<DataFetcherResult<Notebook>>
        result = type.batchLoad(ImmutableList.of(TEST_NOTEBOOK, dummyNotebookUrn.toString()), mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1)).batchGetV2(
        Mockito.eq(Constants.NOTEBOOK_ENTITY_NAME),
        Mockito.eq(ImmutableSet.of(notebookUrn, dummyNotebookUrn)),
        Mockito.eq(NotebookType.ASPECTS_TO_RESOLVE),
        Mockito.any(Authentication.class)
    );

    assertEquals(result.size(), 2);

    System.out.println(result.get(0).getData());

    Notebook notebook = result.get(0).getData();
    assertEquals(notebook.getContent().getCells().size(), NOTEBOOK_CONTENT.getCells().size());
    assertEquals(notebook.getContent().getCells().get(0).getType().toString(),
        NOTEBOOK_CONTENT.getCells().get(0).getType().toString());
    assertEquals(notebook.getContent().getCells().get(0).getTextCell().getCellId(),
        NOTEBOOK_CONTENT.getCells().get(0).getTextCell().getCellId());
    assertEquals(notebook.getContent().getCells().get(0).getTextCell().getCellTitle(),
        NOTEBOOK_CONTENT.getCells().get(0).getTextCell().getCellTitle());
    assertEquals(notebook.getContent().getCells().get(0).getTextCell().getText(),
        NOTEBOOK_CONTENT.getCells().get(0).getTextCell().getText());
    assertEquals(notebook.getInfo().getDescription(), NOTEBOOK_INFO.getDescription());
    assertEquals(notebook.getInfo().getExternalUrl(), NOTEBOOK_INFO.getExternalUrl().toString());
    assertEquals(notebook.getTool(), NOTEBOOK_KEY.getNotebookTool());
    assertEquals(notebook.getNotebookId(), NOTEBOOK_KEY.getNotebookId());
    assertEquals(notebook.getUrn(), TEST_NOTEBOOK);
    assertEquals(notebook.getType(), EntityType.NOTEBOOK);
    assertEquals(notebook.getOwnership().getOwners().size(), 1);
    assertEquals(notebook.getInstitutionalMemory().getElements().size(), 1);
    assertEquals(notebook.getEditableProperties().getDescription(), TEST_EDITABLE_DESCRIPTION.getDescription());
    assertEquals(notebook.getTags().getTags().get(0).getTag().getUrn(),
        GLOBAL_TAGS.getTags().get(0).getTag().toString());
    assertEquals(notebook.getSubTypes().getTypeNames(), SUB_TYPES.getTypeNames().stream().collect(Collectors.toList()));
    assertEquals(notebook.getGlossaryTerms().getTerms().get(0).getTerm().getUrn(),
        TEST_GLOSSARY_TERMS.getTerms().get(0).getUrn().toString());
    assertEquals(notebook.getPlatform().getUrn(), DATA_PLATFORM_INSTANCE.getPlatform().toString());

    // Assert second element is null.
    assertNull(result.get(1));
  }

  @Test
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class).when(mockClient).batchGetV2(
        Mockito.anyString(),
        Mockito.anySet(),
        Mockito.anySet(),
        Mockito.any(Authentication.class));
    ContainerType type = new ContainerType(mockClient);

    // Execute Batch load
    QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    assertThrows(RuntimeException.class, () -> type.batchLoad(ImmutableList.of(TEST_NOTEBOOK),
        context));
  }
}

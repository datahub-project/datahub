package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class CorpUserEditableInfoMutatorTest {

  private EntityRegistry entityRegistry;
  private RetrieverContext mockRetrieverContext;
  private CorpuserUrn testCorpUserUrn;
  private final CorpUserEditableInfoMutator test =
      new CorpUserEditableInfoMutator().setConfig(mock(AspectPluginConfig.class));

  @BeforeTest
  public void init() throws URISyntaxException {
    testCorpUserUrn = CorpuserUrn.createFromUrn(UrnUtils.getUrn("urn:li:corpuser:someuser"));

    entityRegistry = new TestEntityRegistry();
    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    GraphRetriever mockGraphRetriever = mock(GraphRetriever.class);
    mockRetrieverContext = mock(RetrieverContext.class);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockRetrieverContext.getGraphRetriever()).thenReturn(mockGraphRetriever);
  }

  @Test
  public void testValidateIncorrectAspect() {
    final CorpUserInfo corpUserInfo =
        new CorpUserInfo()
            .setActive(true)
            .setTitle("Something")
            .setDisplayName("Name")
            .setFirstName("Some")
            .setLastName("Name")
            .setEmail("a@a.com");
    assertEquals(
        test.writeMutation(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testCorpUserUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testCorpUserUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testCorpUserUrn.getEntityType())
                                .getAspectSpec(CORP_USER_INFO_ASPECT_NAME))
                        .recordTemplate(corpUserInfo)
                        .build()),
                mockRetrieverContext)
            .filter(Pair::getSecond)
            .count(),
        0);
  }

  @Test
  public void testValidatePersonaNonNull() {
    Urn persona = UrnUtils.getUrn("urn:li:dataHubPersona:123456");
    CorpUserEditableInfo oldAspect = new CorpUserEditableInfo().setPersona(persona);
    CorpUserEditableInfo newAspect = new CorpUserEditableInfo();

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                TestMCP.ofOneMCP(testCorpUserUrn, oldAspect, newAspect, entityRegistry),
                mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.stream().filter(Pair::getSecond).count(), 1);
    assertEquals(
        result.get(0).getFirst().getAspect(CorpUserEditableInfo.class).getPersona(), persona);
  }
}

package com.linkedin.metadata.aspect.validation;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Deprecation;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.fabrictype.CustomFabricTypeInfo;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.key.CustomFabricTypeKey;
import com.linkedin.test.metadata.aspect.MockAspectRetriever;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class FabricTypeLookupTest {

  private static final Urn ACTOR = UrnUtils.getUrn("urn:li:corpuser:admin");

  @Test
  public void builtInAndMalformedNamesNeedNoRead() {
    CachingAspectRetriever retriever = mock(CachingAspectRetriever.class);

    Map<String, FabricTypeLookup.Resolution> resolved =
        FabricTypeLookup.resolve(
            OperationFingerprint.EMPTY, Set.of("PROD", "my_env"), context(retriever));

    assertEquals(resolved.get("PROD").getState(), FabricTypeLookup.State.BUILT_IN);
    assertEquals(resolved.get("my_env").getState(), FabricTypeLookup.State.MALFORMED);
    verifyNoInteractions(retriever);
  }

  @Test
  public void customNamesResolveByRegistrationInOneRead() {
    Map<Urn, List<RecordTemplate>> types = new HashMap<>();
    register(types, "ACTIVE_ENV", null, null);
    register(types, "DELETED_ENV", new Status().setRemoved(true), null);
    register(
        types,
        "OLD_ENV",
        null,
        new Deprecation().setDeprecated(true).setNote("Retired.").setActor(ACTOR));
    MockAspectRetriever retriever = spy(new MockAspectRetriever(types));

    Map<String, FabricTypeLookup.Resolution> resolved =
        FabricTypeLookup.resolve(
            OperationFingerprint.EMPTY,
            Set.of("ACTIVE_ENV", "DELETED_ENV", "OLD_ENV", "NEW_ENV", "PROD"),
            context(retriever));

    assertEquals(resolved.get("ACTIVE_ENV").getState(), FabricTypeLookup.State.REGISTERED);
    assertNull(resolved.get("ACTIVE_ENV").getDeprecation());
    assertEquals(resolved.get("DELETED_ENV").getState(), FabricTypeLookup.State.DELETED);
    assertEquals(resolved.get("OLD_ENV").getState(), FabricTypeLookup.State.REGISTERED);
    assertNotNull(resolved.get("OLD_ENV").getDeprecation());
    assertEquals(resolved.get("NEW_ENV").getState(), FabricTypeLookup.State.UNREGISTERED);
    assertEquals(resolved.get("PROD").getState(), FabricTypeLookup.State.BUILT_IN);
    verify(retriever, times(1)).getLatestAspectObjects(any(), anySet(), anySet());
  }

  private static RetrieverContext context(CachingAspectRetriever retriever) {
    return RetrieverContext.builder()
        .graphRetriever(mock(GraphRetriever.class))
        .aspectRetriever(retriever)
        .cachingAspectRetriever(retriever)
        .build();
  }

  private static void register(
      Map<Urn, List<RecordTemplate>> types, String id, Status status, Deprecation deprecation) {
    List<RecordTemplate> aspects = new ArrayList<>();
    aspects.add(new CustomFabricTypeKey().setId(id));
    aspects.add(new CustomFabricTypeInfo().setDisplayName(id));
    if (status != null) {
      aspects.add(status);
    }
    if (deprecation != null) {
      aspects.add(deprecation);
    }
    types.put(FabricTypeLookup.urn(id), aspects);
  }
}

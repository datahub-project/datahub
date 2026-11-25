package com.linkedin.datahub.graphql.types.agent;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.agent.AIAgentToolRef;
import com.linkedin.agent.AIAgentToolRefArray;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.AgentToolRef;
import com.linkedin.datahub.graphql.generated.ToolRefType;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class AgentToolRefMapperTest {

  @Test
  public void testReadSkipsRefTypesGraphqlLacks() {
    final List<AgentToolRef> mapped = AgentToolRefMapper.toGraphql(storedRefs());

    assertEquals(mapped.size(), 3);
    assertEquals(mapped.get(0).getRefType(), ToolRefType.TOOL);
    assertEquals(mapped.get(1).getRefType(), ToolRefType.GROUP);
    assertEquals(mapped.get(2).getRefType(), ToolRefType.AGENT);
    assertEquals(mapped.get(2).getToolId(), "urn:li:aiAgent:helper");
  }

  @Test
  public void testUnrepresentableReturnsSkippedRefs() {
    final List<AIAgentToolRef> kept = AgentToolRefMapper.unrepresentable(storedRefs());

    assertEquals(kept.size(), 2);
    assertEquals(kept.get(0).getRefType(), com.linkedin.agent.ToolRefType.PLUGIN);
    // The raw value of a type from a newer schema is kept as stored.
    assertEquals(kept.get(1).data().get("refType"), "FUTURE_TYPE");
    assertTrue(AgentToolRefMapper.unrepresentable(null).isEmpty());
  }

  private static AIAgentToolRefArray storedRefs() {
    return new AIAgentToolRefArray(
        ref(com.linkedin.agent.ToolRefType.TOOL, "search"),
        ref(com.linkedin.agent.ToolRefType.GROUP, "read_metadata"),
        ref(com.linkedin.agent.ToolRefType.PLUGIN, "github"),
        new AIAgentToolRef(
            new DataMap(Map.of("refType", "FUTURE_TYPE", "toolId", "future", "enabled", true))),
        ref(com.linkedin.agent.ToolRefType.AGENT, "urn:li:aiAgent:helper"));
  }

  private static AIAgentToolRef ref(final com.linkedin.agent.ToolRefType type, final String id) {
    return new AIAgentToolRef().setRefType(type).setToolId(id).setEnabled(true);
  }
}

package com.linkedin.metadata.entity.ebean.batch;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.Map;
import org.testng.annotations.Test;

public class AttributedActorHeadersTest {
  private static final String SYSTEM = "urn:li:corpuser:__datahub_system";
  private static final String ALICE = "urn:li:corpuser:alice";

  private static AuditStamp systemStamp() {
    return new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM)).setTime(42L);
  }

  private static MetadataChangeProposal withHeader(String key, String value) {
    return new MetadataChangeProposal().setHeaders(new StringMap(Map.of(key, value)));
  }

  @Test
  public void trustedHeaderAttributesActor() {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    AttributedActorHeaders.stamp(mcp, ALICE);
    AuditStamp caller = systemStamp();

    AuditStamp stamp = AttributedActorHeaders.resolve(mcp, caller, true);

    assertEquals(stamp.getActor().toString(), ALICE);
    assertEquals(stamp.getImpersonator().toString(), SYSTEM);
    assertEquals(stamp.getTime(), 42L);
    assertNull(caller.getImpersonator(GetMode.NULL));
    assertEquals(caller.getActor().toString(), SYSTEM);
  }

  @Test
  public void untrustedHeaderIgnored() {
    MetadataChangeProposal mcp = withHeader(AttributedActorHeaders.ATTRIBUTED_ACTOR, ALICE);
    AuditStamp caller = systemStamp();

    assertSame(AttributedActorHeaders.resolve(mcp, caller, false), caller);
  }

  @Test
  public void missingHeaderReturnsCaller() {
    AuditStamp caller = systemStamp();

    assertSame(AttributedActorHeaders.resolve(new MetadataChangeProposal(), caller, true), caller);
  }

  @Test
  public void malformedHeaderFallsBack() {
    MetadataChangeProposal mcp = withHeader(AttributedActorHeaders.ATTRIBUTED_ACTOR, "not-a-urn");
    AuditStamp caller = systemStamp();

    assertSame(AttributedActorHeaders.resolve(mcp, caller, true), caller);
  }

  @Test
  public void headerKeyCaseInsensitive() {
    MetadataChangeProposal mcp = withHeader("x-datahub-attributed-actor", ALICE);

    assertEquals(
        AttributedActorHeaders.resolve(mcp, systemStamp(), true).getActor().toString(), ALICE);
  }

  @Test
  public void stampOverwritesExistingValue() {
    MetadataChangeProposal mcp = withHeader("x-datahub-attributed-actor", "urn:li:corpuser:bob");

    AttributedActorHeaders.stamp(mcp, ALICE);

    assertEquals(mcp.getHeaders(), Map.of(AttributedActorHeaders.ATTRIBUTED_ACTOR, ALICE));
  }
}

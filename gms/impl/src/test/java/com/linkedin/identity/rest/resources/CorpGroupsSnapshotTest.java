package com.linkedin.identity.rest.resources;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.CorpGroupUrnArray;
import com.linkedin.common.CorpuserUrnArray;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.metadata.aspect.CorpGroupAspect;
import com.linkedin.metadata.aspect.CorpGroupAspectArray;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.restli.BaseSnapshotResource;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.ResourceContext;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class CorpGroupsSnapshotTest extends BaseEngineTest {

  private BaseLocalDAO<CorpGroupAspect, CorpGroupUrn> _mockLocalDAO;
  private CorpGroupUrn corpGroupUrn = new CorpGroupUrn("foo");

  class CorpGroupSnapshotResource extends BaseSnapshotResource<CorpGroupUrn, CorpGroupSnapshot, CorpGroupAspect> {

    public CorpGroupSnapshotResource() {
      super(CorpGroupSnapshot.class, CorpGroupAspect.class);
    }

    @Nonnull
    @Override
    protected BaseLocalDAO<CorpGroupAspect, CorpGroupUrn> getLocalDAO() {
      return _mockLocalDAO;
    }

    @Nonnull
    @Override
    protected CorpGroupUrn getUrn(@Nonnull PathKeys entityPathKeys) {
      return corpGroupUrn;
    }

    @Override
    public ResourceContext getContext() {
      return mock(ResourceContext.class);
    }
  }

  @BeforeMethod
  public void setup() {
    _mockLocalDAO = mock(BaseLocalDAO.class);
  }

  @Test
  public void testCreate() {

    CorpGroupSnapshotResource resource = new CorpGroupSnapshotResource();
    CorpGroupSnapshot snapshot = new CorpGroupSnapshot();

    CorpGroupAspect aspect = new CorpGroupAspect();
    CorpGroupInfo info = new CorpGroupInfo();
    info.setEmail("foo-dev@linkedin.com");
    CorpuserUrnArray admins = new CorpuserUrnArray();

    CorpuserUrn admin1 = new CorpuserUrn("admin1");
    admins.add(admin1);
    CorpuserUrnArray members = new CorpuserUrnArray();
    CorpuserUrn member1 = new CorpuserUrn("member1");
    CorpuserUrn member2 = new CorpuserUrn("member2");
    members.add(member1);
    members.add(member2);

    CorpGroupUrnArray groups = new CorpGroupUrnArray();
    CorpGroupUrn group1 = new CorpGroupUrn("group1");
    CorpGroupUrn group2 = new CorpGroupUrn("group2");
    CorpGroupUrn group3 = new CorpGroupUrn("group3");

    groups.add(group1);
    groups.add(group2);
    groups.add(group3);

    info.setAdmins(admins);
    info.setMembers(members);
    info.setGroups(groups);

    aspect.setCorpGroupInfo(info);

    CorpGroupAspectArray arr = new CorpGroupAspectArray();
    arr.add(aspect);

    snapshot.setAspects(arr);

    CreateResponse response = runAndWait(resource.create(snapshot));

    assertFalse(response.hasError());
    verify(_mockLocalDAO, times(1)).add(eq(corpGroupUrn), eq(info), any(AuditStamp.class));
  }
}

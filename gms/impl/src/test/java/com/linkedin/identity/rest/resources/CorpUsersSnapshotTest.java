package com.linkedin.identity.rest.resources;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.aspect.AspectVersion;
import com.linkedin.metadata.aspect.AspectVersionArray;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.aspect.CorpUserAspectArray;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.restli.BaseSnapshotResource;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.metadata.snapshot.SnapshotKey;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.ResourceContext;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class CorpUsersSnapshotTest extends BaseEngineTest {

    private BaseLocalDAO<CorpUserAspect, CorpuserUrn> _mockLocalDAO;
    private CorpuserUrn corpUserUrn = new CorpuserUrn("foo");

    class CorpUserSnapshotResource extends BaseSnapshotResource<CorpuserUrn, CorpUserSnapshot, CorpUserAspect> {

        public CorpUserSnapshotResource() {
            super(CorpUserSnapshot.class, CorpUserAspect.class);
        }

        @Nonnull
        @Override
        protected BaseLocalDAO<CorpUserAspect, CorpuserUrn> getLocalDAO() {
            return _mockLocalDAO;
        }

        @Nonnull
        @Override
        protected CorpuserUrn getUrn(@Nonnull PathKeys entityPathKeys) {
            return corpUserUrn;
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
        CorpUserSnapshotResource resource = new CorpUserSnapshotResource();
        CorpUserSnapshot snapshot = new CorpUserSnapshot();
        CorpUserInfo info = new CorpUserInfo();

        info.setFirstName("hang");
        info.setLastName("zhang");
        info.setActive(true);

        CorpUserAspect aspect = new CorpUserAspect();
        aspect.setCorpUserInfo(info);

        CorpUserAspectArray arr = new CorpUserAspectArray();
        arr.add(aspect);

        snapshot.setAspects(arr);

        snapshot.setUrn(corpUserUrn);

        SnapshotKey sKey = new SnapshotKey();
        AspectVersionArray array = new AspectVersionArray();
        AspectVersion v = new AspectVersion();
        v.setAspect(ModelUtils.getAspectName(CorpUserInfo.class));
        v.setVersion(0L);
        array.add(v);
        sKey.setAspectVersions(array);

        CreateResponse response = runAndWait(resource.create(snapshot));

        assertFalse(response.hasError());

        verify(_mockLocalDAO, times(1)).add(eq(corpUserUrn), eq(info), any(AuditStamp.class));
    }
}
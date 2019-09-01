package com.linkedin.datahub.dao.view;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.datahub.util.CorpUserUtil;
import com.linkedin.identity.CorpUser;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.client.CorpUsers;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.List;


@Slf4j
public class CorpUserViewDao {

    private static final String MANAGER_FIELD_NAME = "managerName";

    private final CorpUsers _corpUsers;

    public CorpUserViewDao(@Nonnull CorpUsers corpUsers) {
        this._corpUsers = corpUsers;
    }

    /**
     * Adds manager name to {@link CorpUser} if managerUrn is present in info
     * @param corpUserUrn corpUser urn string
     * @return JSON node corresponding to the {@link CorpUser}
     * @throws Exception
     */
    @Nonnull
    public CorpUser get(@Nonnull String corpUserUrn) throws Exception {
        CorpUser corpUser = _corpUsers.get(CorpUserUtil.toCorpUserUrn(corpUserUrn));
        if (corpUser.getInfo() == null) {
            return corpUser;
        }
        CorpuserUrn managerUrn = corpUser.getInfo().getManagerUrn();
        if (managerUrn == null) {
            return corpUser;
        }
        CorpUser manager = _corpUsers.get(managerUrn);
        String managerName = manager.getInfo() != null ? manager.getInfo().getFullName() : null;
        if (managerName != null) {
            corpUser.getInfo().data().put(MANAGER_FIELD_NAME, managerName);
        }
        return corpUser;
    }

    @Nonnull
    public CorpUser getByUserName(@Nonnull String userName) throws Exception {
        return get(new CorpuserUrn(userName).toString());
    }

    @Nonnull
    public List<CorpUser> getAllCorpUsers() throws Exception {
        return _corpUsers.getAll();
    }

    public void updateCorpUserEditableConfig(@Nonnull String corpUserUrn,
                                             @Nonnull CorpUserEditableInfo corpUserEditableInfo) throws Exception {
        _corpUsers.createEditableInfo(CorpUserUtil.toCorpUserUrn(corpUserUrn), corpUserEditableInfo);
    }
}
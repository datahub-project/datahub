package com.linkedin.datahub.util;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.datahub.models.table.CompanyUser;
import com.linkedin.datahub.models.table.User;
import com.linkedin.datahub.models.table.UserEntity;
import com.linkedin.identity.CorpUser;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;


public class CorpUserUtil {
    private CorpUserUtil() {
    }

    /**
     * Deserialize {@link CorpuserUrn} string
     *
     * @param urnStr urn string
     * @return {@link CorpuserUrn}
     */
    @Nonnull
    public static CorpuserUrn toCorpUserUrn(@Nonnull String urnStr) throws URISyntaxException {
        return CorpuserUrn.createFromString(urnStr);
    }

    /**
     * Convert {@link CorpUser} to {@link User}
     * @param corpUser {@link CorpUser}
     * @return {@link User}
     */
    @Nonnull
    public static User toCorpUserView(CorpUser corpUser) {
        User user = new User();
        user.setUserName(corpUser.getUsername());
        if (corpUser.getInfo() != null) {
            user.setEmail(corpUser.getInfo().getEmail());
            user.setName(corpUser.getInfo().getFullName());
        }
        if (corpUser.getEditableInfo() != null) {
            user.setPictureLink(corpUser.getEditableInfo().getPictureLink().toString());
        }
        return user;
    }

    /**
     * Convert {@link CorpUser} to {@link CompanyUser}
     * @param corpUser {@link CorpUser}
     * @return {@link CompanyUser}
     */
    @Nonnull
    public static CompanyUser toCompanyUserView(CorpUser corpUser) {
        CompanyUser user = new CompanyUser();
        user.setUserName(corpUser.getUsername());
        user.setDisplayName(corpUser.getInfo().getDisplayName());
        user.setPictureLink(corpUser.getEditableInfo().getPictureLink().toString());
        return user;
    }

    /**
     * Convert {@link CorpUser} to {@link UserEntity}
     * @param corpUser {@link CorpUser}
     * @return {@link UserEntity}
     */
    @Nonnull
    public static UserEntity toUserEntityView(CorpUser corpUser) {
        UserEntity user = new UserEntity();
        user.setCategory("person");
        user.setLabel(corpUser.getUsername());
        if (corpUser.hasInfo()) {
            user.setDisplayName(corpUser.getInfo().getDisplayName());
        }
        if (corpUser.hasEditableInfo() && corpUser.getEditableInfo().hasPictureLink()) {
            user.setPictureLink(corpUser.getEditableInfo().getPictureLink().toString());
        }
        return user;
    }
}
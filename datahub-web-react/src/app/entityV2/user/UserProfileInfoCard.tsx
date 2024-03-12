import React, { useState } from 'react';
import { Col } from 'antd';
import { EntityRelationship, SearchResult } from '../../../types.generated';
import SectionActionButton from '../shared/containers/profile/sidebar/SectionActionButton';
import UserEditProfileModal from './UserEditProfileModal';
import CustomAvatar from '../../shared/avatar/CustomAvatar';

import {
    CustomAvatarContainer,
    GradientContainer,
    EditProfileButtonContainer,
    WhiteEditOutlinedIconStyle,
    UserInfo,
} from '../shared/SidebarStyledComponents';
import { UserBasicInfoContainer } from './UserBasicInfoContainer';

const AVATAR_STYLE = {
    marginRight: '0px',
    borderRadius: '100%',
    zIndex: '2',
};

export type SidebarData = {
    photoUrl: string | undefined;
    avatarName: string | undefined;
    name: string | undefined;
    role: string | undefined;
    team: string | undefined;
    email: string | undefined;
    slack: string | undefined;
    phone: string | undefined;
    aboutText: string | undefined;
    groupsDetails: Array<EntityRelationship>;
    ownerships: Array<SearchResult> | undefined;
    urn: string | undefined;
    dataHubRoles: Array<EntityRelationship>;
};

type Props = {
    sidebarData: SidebarData;
    refetch: () => void;
    dataHubRoleName: string;
    isProfileOwner: boolean;
};

export const UserProfileInfoCard = ({ sidebarData, refetch, dataHubRoleName, isProfileOwner }: Props) => {
    const { name, avatarName, email, phone, photoUrl, role, slack, team, urn } = sidebarData;

    const [editProfileModal, showEditProfileModal] = useState(false);

    const getEditModalData = {
        urn,
        name,
        title: role,
        team,
        email,
        image: photoUrl,
        slack,
        phone,
    };

    return (
        <>
            <CustomAvatarContainer>
                <GradientContainer />
                <UserInfo>
                    <Col xxl={8} xl={10} lg={24} md={24} sm={24} xs={24}>
                        <CustomAvatar size={113} photoUrl={photoUrl} name={avatarName} style={AVATAR_STYLE} />
                    </Col>
                    <Col xxl={14} xl={10} lg={18} md={18} sm={18} xs={18}>
                        <UserBasicInfoContainer
                            name={name}
                            dataHubRoleName={dataHubRoleName}
                            email={email}
                            role={role}
                            slack={slack}
                            phone={phone}
                        />
                    </Col>
                    {isProfileOwner && (
                        <EditProfileButtonContainer className="edit-button-container">
                            <SectionActionButton
                                button={<WhiteEditOutlinedIconStyle />}
                                onClick={(event) => {
                                    showEditProfileModal(true);
                                    event.stopPropagation();
                                }}
                            />
                        </EditProfileButtonContainer>
                    )}
                </UserInfo>
            </CustomAvatarContainer>
            {/* Modal */}
            <UserEditProfileModal
                visible={editProfileModal}
                onClose={() => showEditProfileModal(false)}
                onSave={() => {
                    refetch();
                }}
                editModalData={getEditModalData}
            />
        </>
    );
};

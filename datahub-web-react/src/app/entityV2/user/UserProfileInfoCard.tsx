import { Avatar } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';

import {
    CustomAvatarContainer,
    EditProfileButtonContainer,
    GradientContainer,
    UserInfo,
    WhiteEditOutlinedIconStyle,
} from '@app/entityV2/shared/SidebarStyledComponents';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { UserBasicInfoContainer } from '@app/entityV2/user/UserBasicInfoContainer';
import UserEditProfileModal from '@app/entityV2/user/UserEditProfileModal';

import { EntityRelationship, SearchResults } from '@types';

const ProfileAvatarWrapper = styled.div`
    position: relative;
    z-index: 2;
`;

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
    ownershipResults: SearchResults | undefined;
    urn: string | undefined;
    dataHubRoles: Array<EntityRelationship>;
};

// Column widths match the antd Col spans they replace.
const AvatarColumn = styled.div`
    position: relative;
    min-height: 1px;
    flex: 0 0 100%;
    max-width: 100%;

    @media (min-width: 1200px) {
        flex: 0 0 41.66666667%;
        max-width: 41.66666667%;
    }

    @media (min-width: 1600px) {
        flex: 0 0 33.33333333%;
        max-width: 33.33333333%;
    }
`;

const InfoColumn = styled.div`
    position: relative;
    min-height: 1px;
    flex: 0 0 75%;
    max-width: 75%;

    @media (min-width: 1200px) {
        flex: 0 0 41.66666667%;
        max-width: 41.66666667%;
    }

    @media (min-width: 1600px) {
        flex: 0 0 58.33333333%;
        max-width: 58.33333333%;
    }
`;

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
                    <AvatarColumn>
                        <ProfileAvatarWrapper>
                            <Avatar name={avatarName || ''} imageUrl={photoUrl} type={AvatarType.user} size="3xl" />
                        </ProfileAvatarWrapper>
                    </AvatarColumn>
                    <InfoColumn>
                        <UserBasicInfoContainer
                            name={name}
                            dataHubRoleName={dataHubRoleName}
                            email={email}
                            role={role}
                            slack={slack}
                            phone={phone}
                        />
                    </InfoColumn>
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

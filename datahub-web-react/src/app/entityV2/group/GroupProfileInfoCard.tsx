import { Col, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { GroupBasicInfoSection } from '@app/entityV2/group/GroupBasicInfoSection';
import GroupEditModal from '@app/entityV2/group/GroupEditModal';
import { GroupInfoHeaderSection } from '@app/entityV2/group/GroupInfoHeaderSection';
import {
    CustomAvatarContainer,
    EditProfileButtonContainer,
    GroupInfo,
    WhiteEditOutlinedIconStyle,
} from '@app/entityV2/shared/SidebarStyledComponents';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import CustomAvatar from '@app/shared/avatar/CustomAvatar';

import { useUpdateNameMutation } from '@graphql/mutations.generated';
import { EntityRelationshipsResult, Ownership } from '@types';

export type SidebarData = {
    photoUrl: string | undefined;
    avatarName: string | undefined;
    name: string | undefined;
    email: string | undefined;
    slack: string | undefined;
    aboutText: string | undefined;
    groupMemberRelationships: EntityRelationshipsResult;
    groupOwnership: Ownership;
    isExternalGroup: boolean;
    externalGroupType: string | undefined;
    urn: string;
};

const AVATAR_STYLE = {
    borderRadius: '9px',
    zIndex: '2',
    height: '36px',
    width: '36px',
    backgroundColor: REDESIGN_COLORS.AVATAR_STYLE_WHITE_BACKGROUND,
};

const AvatarWithTitleContainer = styled.div`
    display: flex;
    padding: 10px;
    background: ${REDESIGN_COLORS.GROUP_AVATAR_STYLE_GRADIENT}};
    gap: 0.5rem;
`;

type Props = {
    sidebarData: SidebarData;
    refetch: () => Promise<any>;
};

export const GroupProfileInfoCard = ({ sidebarData, refetch }: Props) => {
    const {
        avatarName,
        name,
        groupMemberRelationships,
        email,
        photoUrl,
        slack,
        isExternalGroup,
        externalGroupType,
        urn,
    } = sidebarData;

    const [updateName] = useUpdateNameMutation();
    const [editGroupModal, showEditGroupModal] = useState(false);

    const me = useUserContext();
    const canEditGroupName = me?.platformPrivileges?.manageIdentities;

    // Update Group Title
    // eslint-disable-next-line @typescript-eslint/no-shadow
    const handleTitleUpdate = async (name: string) => {
        await updateName({ variables: { input: { name, urn } } })
            .then(() => {
                message.success({ content: 'Name Updated', duration: 2 });
                refetch();
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to update name: \n ${e.message || ''}`, duration: 3 });
                }
            });
    };

    const getEditModalData = {
        name,
        urn,
        email,
        slack,
    };

    return (
        <>
            <CustomAvatarContainer>
                <GroupInfo>
                    <AvatarWithTitleContainer>
                        <Col xxl={2} xl={3} lg={4} md={4} sm={3} xs={3}>
                            <CustomAvatar
                                useDefaultAvatar={false}
                                size={36}
                                photoUrl={photoUrl}
                                name={avatarName}
                                style={AVATAR_STYLE}
                            />
                        </Col>
                        <Col xxl={20} xl={18} lg={16} md={16} sm={19} xs={19}>
                            <GroupInfoHeaderSection
                                groupName={name}
                                groupMemberRelationships={groupMemberRelationships}
                                isExternalGroup={isExternalGroup}
                                externalGroupType={externalGroupType}
                            />
                        </Col>
                        <EditProfileButtonContainer className="edit-button-container">
                            <SectionActionButton
                                button={<WhiteEditOutlinedIconStyle />}
                                onClick={(event) => {
                                    showEditGroupModal(true);
                                    event.stopPropagation();
                                }}
                            />
                        </EditProfileButtonContainer>
                    </AvatarWithTitleContainer>
                    <GroupBasicInfoSection email={email} slack={slack} />
                </GroupInfo>
            </CustomAvatarContainer>
            {/* Modal */}
            <GroupEditModal
                canEditGroupName={canEditGroupName}
                visible={editGroupModal}
                handleTitleUpdate={handleTitleUpdate}
                onClose={() => showEditGroupModal(false)}
                onSave={() => {
                    refetch();
                }}
                editModalData={getEditModalData}
            />
        </>
    );
};

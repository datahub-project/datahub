import { message } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { GroupProfileInfoCard, SidebarData } from '@app/entityV2/group/GroupProfileInfoCard';
import { GroupSidebarMembersSection } from '@app/entityV2/group/GroupSidebarMembersSection';
import { GroupSidebarOwnersSection } from '@app/entityV2/group/GroupSidebarOwnersSection';
import { Content, SideBar } from '@app/entityV2/shared/SidebarStyledComponents';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { AboutSidebarSection } from '@app/entityV2/shared/sidebarSection/AboutSidebarSection';

import { useUpdateCorpGroupPropertiesMutation } from '@graphql/group.generated';

type Props = {
    sidebarData: SidebarData;
    refetch: () => Promise<any>;
};

export const MemberCount = styled.div`
    font-size: 10px;
    color: ${REDESIGN_COLORS.WHITE};
    font-weight: 400;
    text-align: left;
`;

/**
 * Responsible for reading & writing users.
 */
export default function GroupSidebar({ sidebarData, refetch }: Props) {
    const { aboutText, groupMemberRelationships, urn, groupOwnership: ownership } = sidebarData;
    const [updateCorpGroupPropertiesMutation] = useUpdateCorpGroupPropertiesMutation();

    // About Text save
    const onSaveAboutMe = (inputString) => {
        updateCorpGroupPropertiesMutation({
            variables: {
                urn: urn || '',
                input: {
                    description: inputString,
                },
            },
        })
            .then(() => {
                message.success({
                    content: `Changes saved.`,
                    duration: 3,
                });
                refetch();
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to Save changes!: \n ${e.message || ''}`, duration: 3 });
            });
    };

    return (
        <SideBar>
            <GroupProfileInfoCard sidebarData={sidebarData} refetch={refetch} />
            <Content>
                <AboutSidebarSection aboutText={aboutText || ''} isProfileOwner onSaveAboutMe={onSaveAboutMe} />
                <GroupSidebarOwnersSection ownership={ownership} refetch={refetch} urn={urn} />
                <GroupSidebarMembersSection groupMemberRelationships={groupMemberRelationships} />
            </Content>
        </SideBar>
    );
}

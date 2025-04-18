import { message } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { useUpdateCorpGroupPropertiesMutation } from '../../../graphql/group.generated';
import { SideBar, Content } from '../shared/SidebarStyledComponents';
import { AboutSidebarSection } from '../shared/sidebarSection/AboutSidebarSection';
import { REDESIGN_COLORS } from '../shared/constants';
import { GroupProfileInfoCard, SidebarData } from './GroupProfileInfoCard';
import { GroupSidebarOwnersSection } from './GroupSidebarOwnersSection';
import { GroupSidebarMembersSection } from './GroupSidebarMembersSection';

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

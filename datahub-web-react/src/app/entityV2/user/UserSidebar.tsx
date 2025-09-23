import { message } from 'antd';
import React from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { Content, SideBar } from '@app/entityV2/shared/SidebarStyledComponents';
import { AboutSidebarSection } from '@app/entityV2/shared/sidebarSection/AboutSidebarSection';
import { UserGroupSideBarSection } from '@app/entityV2/shared/sidebarSection/UserGroupSidebarSection';
import { UserOwnershipSidebarSection } from '@app/entityV2/shared/sidebarSection/UserOwnershipSideBarSection';
import { SidebarData, UserProfileInfoCard } from '@app/entityV2/user/UserProfileInfoCard';

import { useUpdateCorpUserPropertiesMutation } from '@graphql/user.generated';
import { DataHubRole } from '@types';

type Props = {
    sidebarData: SidebarData;
    refetch: () => void;
};

/**
 * UserSidebar- Sidebar section for users profiles.
 */
export default function UserSidebar({ sidebarData, refetch }: Props) {
    const { aboutText, groupsDetails, dataHubRoles, urn, ownershipResults } = sidebarData;

    const [updateCorpUserPropertiesMutation] = useUpdateCorpUserPropertiesMutation();

    /* eslint-disable @typescript-eslint/no-unused-vars */
    const me = useUserContext();
    const isProfileOwner = me?.user?.urn === urn;

    // About Text save
    const onSaveAboutMe = (inputString) => {
        updateCorpUserPropertiesMutation({
            variables: {
                urn: urn || '',
                input: {
                    aboutMe: inputString,
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

    const dataHubRoleName =
        dataHubRoles && dataHubRoles.length > 0 ? (dataHubRoles[0]?.entity as DataHubRole).name : '';

    return (
        <SideBar>
            <UserProfileInfoCard
                sidebarData={sidebarData}
                refetch={refetch}
                dataHubRoleName={dataHubRoleName}
                isProfileOwner={isProfileOwner}
            />
            <Content>
                <AboutSidebarSection
                    aboutText={aboutText || ''}
                    isProfileOwner={isProfileOwner}
                    onSaveAboutMe={onSaveAboutMe}
                />
                <UserOwnershipSidebarSection ownershipResults={ownershipResults} />
                <UserGroupSideBarSection groupsDetails={groupsDetails} />
            </Content>
        </SideBar>
    );
}

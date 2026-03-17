import { Col, Row } from 'antd';
import React from 'react';
import styled from 'styled-components';

import NonExistentEntityPage from '@app/entity/shared/entity/NonExistentEntityPage';
import { decodeUrn } from '@app/entity/shared/utils';
import { UserAssets } from '@app/entity/user/UserAssets';
import UserGroups from '@app/entity/user/UserGroups';
import UserInfoSideBar from '@app/entity/user/UserInfoSideBar';
import { RoutedTabs } from '@app/shared/RoutedTabs';
import useUserParams from '@app/shared/entitySearch/routingUtils/useUserParams';
import { ErrorSection } from '@app/shared/error/ErrorSection';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetUserQuery } from '@graphql/user.generated';
import { CorpGroup, EntityRelationship, EntityType } from '@types';

export interface Props {
    onTabChange: (selectedTab: string) => void;
}

export enum TabType {
    Assets = 'Owner Of',
    Groups = 'Groups',
}
const ENABLED_TAB_TYPES = [TabType.Assets, TabType.Groups];

const GROUP_PAGE_SIZE = 20;

const Content = styled.div`
    color: ${(props) => props.theme.colors.text};
    height: calc(100vh - 60px);
`;

const ContentColumn = styled(Col)`
    border-left: 1px solid ${(props) => props.theme.colors.border};
`;

export const EmptyValue = styled.div`
    &:after {
        content: 'None';
        color: ${(props) => props.theme.colors.textTertiary};
        font-style: italic;
        font-weight: 100;
    }
`;

/**
 * Responsible for reading & writing users.
 */
export default function UserProfile() {
    const { urn: encodedUrn } = useUserParams();
    const urn = decodeUrn(encodedUrn);
    const entityRegistry = useEntityRegistry();

    const { error, data, refetch } = useGetUserQuery({ variables: { urn, groupsCount: GROUP_PAGE_SIZE } });

    const corpUser = data?.corpUser;

    // Filter out soft-deleted or orphaned groups that lack both info and editableProperties
    const userGroups: Array<EntityRelationship> =
        corpUser?.groups?.relationships
            ?.filter((relationship) => {
                const group = relationship?.entity as CorpGroup | undefined;
                return group?.info || group?.editableProperties;
            })
            ?.map((relationship) => relationship as EntityRelationship) || [];
    const userRoles: Array<EntityRelationship> =
        corpUser?.roles?.relationships?.map((relationship) => relationship as EntityRelationship) || [];

    // Routed Tabs Constants
    const getTabs = () => {
        return [
            {
                name: TabType.Assets,
                path: TabType.Assets.toLocaleLowerCase(),
                content: <UserAssets urn={urn} />,
                display: {
                    enabled: () => true,
                },
            },
            {
                name: TabType.Groups,
                path: TabType.Groups.toLocaleLowerCase(),
                content: <UserGroups urn={urn} initialRelationships={userGroups} pageSize={GROUP_PAGE_SIZE} />,
                display: {
                    enabled: () => userGroups?.length > 0,
                },
            },
        ].filter((tab) => ENABLED_TAB_TYPES.includes(tab.name));
    };
    const defaultTabPath = getTabs() && getTabs()?.length > 0 ? getTabs()[0].path : '';
    const onTabChange = () => null;

    // Side bar data
    const sideBarData = {
        photoUrl: data?.corpUser?.editableProperties?.pictureLink || undefined,
        avatarName:
            data?.corpUser?.editableProperties?.displayName ||
            data?.corpUser?.info?.displayName ||
            data?.corpUser?.info?.fullName ||
            data?.corpUser?.urn,
        name:
            data?.corpUser?.editableProperties?.displayName ||
            (data?.corpUser && entityRegistry.getDisplayName(EntityType.CorpUser, data?.corpUser)) ||
            undefined,
        role: data?.corpUser?.editableProperties?.title || data?.corpUser?.info?.title || undefined,
        team: data?.corpUser?.editableProperties?.teams?.join(',') || data?.corpUser?.info?.departmentName || undefined,
        countryCode: data?.corpUser?.info?.countryCode || undefined,
        email: data?.corpUser?.editableProperties?.email || data?.corpUser?.info?.email || undefined,
        slack: data?.corpUser?.editableProperties?.slack || undefined,
        phone: data?.corpUser?.editableProperties?.phone || undefined,
        aboutText: data?.corpUser?.editableProperties?.aboutMe || undefined,
        groupsDetails: userGroups,
        dataHubRoles: userRoles,
        urn,
        username: data?.corpUser?.username,
    };

    if (data?.corpUser?.exists === false) {
        return <NonExistentEntityPage />;
    }

    return (
        <>
            {error && <ErrorSection />}
            <Row>
                <Col xl={5} lg={5} md={5} sm={24} xs={24}>
                    <UserInfoSideBar sideBarData={sideBarData} refetch={refetch} />
                </Col>
                <ContentColumn xl={19} lg={19} md={19} sm={24} xs={24}>
                    <Content>
                        <RoutedTabs defaultPath={defaultTabPath} tabs={getTabs()} onTabChange={onTabChange} />
                    </Content>
                </ContentColumn>
            </Row>
        </>
    );
}

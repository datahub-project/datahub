import { Col } from 'antd';
import React, { useContext, useState } from 'react';
import { ReadOutlined } from '@ant-design/icons';
import colors from '@src/alchemy-components/theme/foundations/colors';
import { matchPath } from 'react-router';
import { useLocation } from 'react-router-dom';
import styled from 'styled-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { PageRoutes } from '../../../conf/Global';
import { useGetUserOwnedAssetsQuery, useGetUserQuery } from '../../../graphql/user.generated';
import { EntityRelationship, EntityType } from '../../../types.generated';
import { EntityContext } from '../../entity/shared/EntityContext';
import { EntityHead } from '../../shared/EntityHead';
import { GenericEntityProperties } from '../../entity/shared/types';
import UserGroups from './UserGroups';
import { RoutedTabs } from '../../shared/RoutedTabs';
import { UserAssets } from './UserAssets';
import UserSideBar from './UserSidebar';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ErrorSection } from '../../shared/error/ErrorSection';
import { StyledEntitySidebarContainer, StyledSidebar } from '../shared/containers/profile/sidebar/EntityProfileSidebar';
import CompactContext from '../../shared/CompactContext';
import { EntitySidebarTabs } from '../shared/containers/profile/sidebar/EntitySidebarTabs';
import EntitySidebarSectionsTab from '../shared/containers/profile/sidebar/EntitySidebarSectionsTab';
import EntitySidebarContext from '../../sharedV2/EntitySidebarContext';
import SidebarCollapsibleHeader from '../shared/containers/profile/sidebar/SidebarCollapsibleHeader';

export interface Props {
    urn: string;
}

export enum TabType {
    Assets = 'Owner Of',
    Groups = 'Groups',
}

const ENABLED_TAB_TYPES = [TabType.Assets, TabType.Groups];

const GROUP_PAGE_SIZE = 20;

const defaultTabDisplayConfig = {
    visible: (_, _1) => true,
    enabled: (_, _1) => true,
};

/**
 * Styled Components
 */
const UserProfileWrapper = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    &&& .ant-tabs-nav {
        margin: 0;
    }
    background-color: #fff;
    height: 100%;
    overflow: hidden;
    display: flex;
    &&& .ant-tabs > .ant-tabs-nav .ant-tabs-nav-wrap {
        padding-left: 15px;
    }

    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};
        margin: 5px;
    `}
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
`;

export const EmptyValue = styled.div`
    &:after {
        content: 'None';
        color: #b7b7b7;
        font-style: italic;
        font-weight: 100;
    }
`;

const ContentContainer = styled.div<{ isVisible: boolean }>`
    flex: 1;
    ${(props) => props.isVisible && 'border-right: 1px solid #e8e8e8;'}
    overflow: inherit;
`;

const TabsContainer = styled.div``;

const Tabs = styled.div``;

/**
 * Responsible for reading & writing users.
 */
export default function UserProfile({ urn }: Props) {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const entityRegistry = useEntityRegistry();
    const location = useLocation();
    const isCompact = React.useContext(CompactContext);
    const isInSearch = matchPath(location.pathname, PageRoutes.SEARCH_RESULTS) !== null;

    const { error, data, loading, refetch } = useGetUserQuery({ variables: { urn, groupsCount: GROUP_PAGE_SIZE } });

    const castedCorpUser = data?.corpUser as any;

    const userGroups: Array<EntityRelationship> =
        castedCorpUser?.groups?.relationships?.map((relationship) => relationship as EntityRelationship) || [];
    const userRoles: Array<EntityRelationship> =
        castedCorpUser?.roles?.relationships?.map((relationship) => relationship as EntityRelationship) || [];

    const { data: userOwnedAsset } = useGetUserOwnedAssetsQuery({ variables: { urn } });
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

    const displayName =
        data?.corpUser?.editableProperties?.displayName ||
        (data?.corpUser && entityRegistry.getDisplayName(EntityType.CorpUser, data?.corpUser)) ||
        undefined;

    // Side bar data
    const sidebarData = {
        photoUrl: data?.corpUser?.editableProperties?.pictureLink || undefined,
        avatarName:
            data?.corpUser?.editableProperties?.displayName ||
            data?.corpUser?.info?.displayName ||
            data?.corpUser?.info?.fullName ||
            data?.corpUser?.urn,
        name: displayName,
        role: data?.corpUser?.editableProperties?.title || data?.corpUser?.info?.title || undefined,
        team: data?.corpUser?.editableProperties?.teams?.join(',') || data?.corpUser?.info?.departmentName || undefined,
        email: data?.corpUser?.editableProperties?.email || data?.corpUser?.info?.email || undefined,
        slack: data?.corpUser?.editableProperties?.slack || undefined,
        phone: data?.corpUser?.editableProperties?.phone || undefined,
        aboutText: data?.corpUser?.editableProperties?.aboutMe || undefined,
        groupsDetails: userGroups,
        dataHubRoles: userRoles,
        ownerships: userOwnedAsset?.searchAcrossEntities?.searchResults || undefined,
        urn,
    };

    const finalTabs = [
        {
            name: 'About',
            icon: ReadOutlined,
            component: EntitySidebarSectionsTab,
            display: {
                ...defaultTabDisplayConfig,
            },
        },
    ];

    const [selectedTabName, setSelectedTabName] = useState(finalTabs[0].name);
    const selectedTab = finalTabs.find((tab) => tab.name === selectedTabName);
    const { width, isClosed } = useContext(EntitySidebarContext);

    if (isCompact) {
        return (
            <StyledEntitySidebarContainer isCollapsed={isClosed} $width={width} isFocused={isInSearch}>
                <StyledSidebar isCard={isInSearch} isFocused={isInSearch}>
                    <ContentContainer isVisible={!isClosed}>
                        <SidebarCollapsibleHeader currentTab={selectedTab} />
                        {!isClosed && <UserSideBar sidebarData={sidebarData} refetch={refetch} />}
                    </ContentContainer>
                    <TabsContainer>
                        <Tabs>
                            <EntitySidebarTabs
                                tabs={finalTabs}
                                selectedTab={selectedTab}
                                onSelectTab={(name) => setSelectedTabName(name)}
                            />
                        </Tabs>
                    </TabsContainer>
                </StyledSidebar>
            </StyledEntitySidebarContainer>
        );
    }

    return (
        <EntityContext.Provider
            value={{
                urn,
                loading,
                refetch,
                entityType: EntityType.CorpUser,
                entityData: (data?.corpUser ?? null) as GenericEntityProperties | null,
                routeToTab: () => {},
                dataNotCombinedWithSiblings: null,
                baseEntity: null,
            }}
        >
            <EntityHead />
            {error && <ErrorSection />}
            <UserProfileWrapper $isShowNavBarRedesign={isShowNavBarRedesign}>
                <Col xl={7} lg={7} md={7} sm={24} xs={24} style={{ height: '100%', overflow: 'auto' }}>
                    <UserSideBar sidebarData={sidebarData} refetch={refetch} />
                </Col>
                <Col
                    xl={17}
                    lg={17}
                    md={17}
                    sm={24}
                    xs={24}
                    style={{ borderLeft: `1px solid ${colors.gray[100]}`, height: '100%' }}
                >
                    <RoutedTabs defaultPath={defaultTabPath} tabs={getTabs()} onTabChange={onTabChange} />
                </Col>
            </UserProfileWrapper>
        </EntityContext.Provider>
    );
}

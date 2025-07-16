import { BookOpen } from '@phosphor-icons/react';
import { Col } from 'antd';
import React, { useContext, useState } from 'react';
import { matchPath } from 'react-router';
import { useLocation } from 'react-router-dom';
import styled from 'styled-components/macro';

import { EntityContext } from '@app/entity/shared/EntityContext';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { GroupAssets } from '@app/entityV2/group/GroupAssets';
import GroupMembers from '@app/entityV2/group/GroupMembers';
import GroupSidebar from '@app/entityV2/group/GroupSidebar';
import { TabType } from '@app/entityV2/group/types';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import {
    StyledEntitySidebarContainer,
    StyledSidebar,
} from '@app/entityV2/shared/containers/profile/sidebar/EntityProfileSidebar';
import EntitySidebarSectionsTab from '@app/entityV2/shared/containers/profile/sidebar/EntitySidebarSectionsTab';
import { EntitySidebarTabs } from '@app/entityV2/shared/containers/profile/sidebar/EntitySidebarTabs';
import SidebarCollapsibleHeader from '@app/entityV2/shared/containers/profile/sidebar/SidebarCollapsibleHeader';
import NonExistentEntityPage from '@app/entityV2/shared/entity/NonExistentEntityPage';
import CompactContext from '@app/shared/CompactContext';
import { EntityHead } from '@app/shared/EntityHead';
import { Message } from '@app/shared/Message';
import { RoutedTabs } from '@app/shared/RoutedTabs';
import { ErrorSection } from '@app/shared/error/ErrorSection';
import EntitySidebarContext from '@app/sharedV2/EntitySidebarContext';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { PageRoutes } from '@conf/Global';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { useGetGroupQuery } from '@graphql/group.generated';
import { EntityRelationshipsResult, EntityType, OriginType, Ownership } from '@types';

const messageStyle = { marginTop: '10%' };

const ENABLED_TAB_TYPES = [TabType.Assets, TabType.Members];

const MEMBER_PAGE_SIZE = 15;

/**
 * Styled Components
 */
const GroupProfileWrapper = styled.div`
    &&& .ant-tabs-nav {
        margin: 0;
    }

    background-color: ${REDESIGN_COLORS.WHITE};
    border-radius: 8px;
    overflow: hidden;
    height: 100%;
    display: flex;
    &&& .ant-tabs > .ant-tabs-nav .ant-tabs-nav-wrap {
        padding-left: 15px;
    }
`;

const ContentContainer = styled.div<{ isVisible: boolean }>`
    flex: 1;
    ${(props) => props.isVisible && `border-right: 1px solid ${REDESIGN_COLORS.SIDE_BAR_BORDER_RIGHT};`}
    overflow: inherit;
`;

const TabsContainer = styled.div``;

const Tabs = styled.div``;

type Props = {
    urn: string;
};

/**
 * Responsible for reading & writing groups.
 *
 * TODO: Add use of apollo cache to improve fetching performance.
 */
export default function GroupProfile({ urn }: Props) {
    const entityRegistry = useEntityRegistry();
    const location = useLocation();
    const isCompact = React.useContext(CompactContext);
    const isInSearch = matchPath(location.pathname, PageRoutes.SEARCH_RESULTS) !== null;
    const { loading, error, data, refetch } = useGetGroupQuery({ variables: { urn, membersCount: MEMBER_PAGE_SIZE } });

    const groupMemberRelationships = data?.corpGroup?.relationships as EntityRelationshipsResult;
    const isExternalGroup: boolean = data?.corpGroup?.origin?.type === OriginType.External;
    const externalGroupType: string = data?.corpGroup?.origin?.externalType || 'outside DataHub';
    const groupName = data?.corpGroup ? entityRegistry.getDisplayName(EntityType.CorpGroup, data.corpGroup) : undefined;

    const finalTabs = [
        {
            name: 'About',
            icon: BookOpen,
            component: EntitySidebarSectionsTab,
            display: {
                visible: () => true,
                enabled: () => true,
            },
        },
    ];

    const [selectedTabName, setSelectedTabName] = useState(finalTabs[0].name);
    const selectedTab = finalTabs.find((tab) => tab.name === selectedTabName);
    const { width, isClosed } = useContext(EntitySidebarContext);

    const getTabs = () => {
        return [
            {
                name: TabType.Assets,
                path: TabType.Assets.toLocaleLowerCase(),
                content: <GroupAssets urn={urn} />,
                display: {
                    enabled: () => true,
                },
            },
            {
                name: TabType.Members,
                path: TabType.Members.toLocaleLowerCase(), // do not remove toLocaleLowerCase as we link to this tab elsewhere
                content: (
                    <GroupMembers
                        urn={urn}
                        pageSize={MEMBER_PAGE_SIZE}
                        isExternalGroup={isExternalGroup}
                        onChangeMembers={() => {
                            setTimeout(() => refetch(), 3000);
                        }}
                    />
                ),
                display: {
                    enabled: () => true,
                },
            },
        ].filter((tab) => ENABLED_TAB_TYPES.includes(tab.name));
    };

    const defaultTabPath = getTabs() && getTabs()?.length > 0 ? getTabs()[0].path : '';
    const onTabChange = () => null;

    // Side bar data
    const sidebarData = {
        photoUrl: undefined,
        avatarName:
            data?.corpGroup?.properties?.displayName ||
            data?.corpGroup?.name ||
            data?.corpGroup?.info?.displayName ||
            undefined,
        name: groupName,
        email: data?.corpGroup?.editableProperties?.email || data?.corpGroup?.properties?.email || undefined,
        slack: data?.corpGroup?.editableProperties?.slack || data?.corpGroup?.properties?.slack || undefined,
        aboutText:
            data?.corpGroup?.editableProperties?.description || data?.corpGroup?.properties?.description || undefined,
        groupMemberRelationships: groupMemberRelationships as EntityRelationshipsResult,
        groupOwnership: data?.corpGroup?.ownership as Ownership,
        isExternalGroup,
        externalGroupType,
        urn,
    };

    if (data?.corpGroup?.exists === false) {
        return <NonExistentEntityPage />;
    }

    if (isCompact) {
        return (
            <StyledEntitySidebarContainer isCollapsed={isClosed} $width={width} isFocused={isInSearch}>
                <StyledSidebar isCard={isInSearch} isFocused={isInSearch}>
                    <ContentContainer isVisible={!isClosed}>
                        <SidebarCollapsibleHeader currentTab={selectedTab} />
                        {!isClosed && <GroupSidebar sidebarData={sidebarData} refetch={refetch} />}
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
                entityType: EntityType.CorpGroup,
                entityData: (data?.corpGroup ?? null) as GenericEntityProperties | null,
                routeToTab: () => {},
                dataNotCombinedWithSiblings: null,
                baseEntity: null,
            }}
        >
            <EntityHead />
            {error && <ErrorSection />}
            {loading && <Message type="loading" content="Loading..." style={messageStyle} />}
            {data && data?.corpGroup && (
                <GroupProfileWrapper>
                    <Col xl={7} lg={7} md={7} sm={24} xs={24} style={{ height: '100%', overflow: 'auto' }}>
                        <GroupSidebar sidebarData={sidebarData} refetch={refetch} />
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
                </GroupProfileWrapper>
            )}
        </EntityContext.Provider>
    );
}

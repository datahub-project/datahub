import React, { useContext, useState } from 'react';
import { Col } from 'antd';
import { matchPath } from 'react-router';
import { useLocation } from 'react-router-dom';
import styled from 'styled-components/macro';
import { ReadOutlined } from '@ant-design/icons';
import colors from '@src/alchemy-components/theme/foundations/colors';
import { PageRoutes } from '../../../conf/Global';
import { useGetGroupQuery } from '../../../graphql/group.generated';
import { OriginType, EntityRelationshipsResult, Ownership, EntityType } from '../../../types.generated';
import { EntityContext } from '../../entity/shared/EntityContext';
import { EntityHead } from '../../shared/EntityHead';
import { GenericEntityProperties } from '../../entity/shared/types';
import { Message } from '../../shared/Message';
import GroupMembers from './GroupMembers';
import { RoutedTabs } from '../../shared/RoutedTabs';
import GroupSidebar from './GroupSidebar';
import { GroupAssets } from './GroupAssets';
import { ErrorSection } from '../../shared/error/ErrorSection';
import { useEntityRegistry } from '../../useEntityRegistry';
import NonExistentEntityPage from '../shared/entity/NonExistentEntityPage';
import CompactContext from '../../shared/CompactContext';
import { StyledEntitySidebarContainer, StyledSidebar } from '../shared/containers/profile/sidebar/EntityProfileSidebar';
import EntitySidebarSectionsTab from '../shared/containers/profile/sidebar/EntitySidebarSectionsTab';
import EntitySidebarContext from '../../sharedV2/EntitySidebarContext';
import SidebarCollapsibleHeader from '../shared/containers/profile/sidebar/SidebarCollapsibleHeader';
import { EntitySidebarTabs } from '../shared/containers/profile/sidebar/EntitySidebarTabs';
import { REDESIGN_COLORS } from '../shared/constants';

const messageStyle = { marginTop: '10%' };

export enum TabType {
    Assets = 'Owner Of',
    Members = 'Members',
}

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

const defaultTabDisplayConfig = {
    visible: (_, _1) => true,
    enabled: (_, _1) => true,
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
                path: TabType.Members.toLocaleLowerCase(),
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

import React, { useContext, useState } from 'react';

import { matchPath, useLocation } from 'react-router';
import { ReadOutlined } from '@ant-design/icons';
import styled from 'styled-components';

import { PageRoutes } from '../../../conf/Global';
import EntitySidebarContext from '../../sharedV2/EntitySidebarContext';
import TagStyleEntity from '../../shared/TagStyleEntity';
import { StyledEntitySidebarContainer, StyledSidebar } from '../shared/containers/profile/sidebar/EntityProfileSidebar';
import { EntitySidebarTab } from '../shared/types';
import EntitySidebarSectionsTab from '../shared/containers/profile/sidebar/EntitySidebarSectionsTab';
import { EntitySidebarTabs } from '../shared/containers/profile/sidebar/EntitySidebarTabs';
import { defaultTabDisplayConfig } from '../shared/containers/profile/utils';

const CompactEntityWrapper = styled.div<{ $isClosed: boolean }>`
    ${(props) => !props.$isClosed && 'padding: 16px;'}
    border-right: 1px solid #e8e8e8;
    flex: 1;
    overflow: inherit;
`;

const TabsContainer = styled.div``;

interface Props {
    urn: string;
}

/**
 * Responsible for displaying metadata about a tag
 */
export default function CompactTagProfile({ urn }: Props) {
    const location = useLocation();
    const isInSearch = matchPath(location.pathname, PageRoutes.SEARCH_RESULTS) !== null;
    const { isClosed, width } = useContext(EntitySidebarContext);

    const tabs: EntitySidebarTab[] = [
        {
            name: 'About',
            icon: ReadOutlined,
            component: EntitySidebarSectionsTab,
            display: defaultTabDisplayConfig,
        },
    ];

    const [selectedTabName, setSelectedTabName] = useState(tabs[0].name);
    const selectedTab = tabs.find((tab) => tab.name === selectedTabName);

    return (
        <StyledEntitySidebarContainer
            isCollapsed={isClosed}
            $width={width}
            id="entity-profile-sidebar"
            isFocused={isInSearch}
        >
            <StyledSidebar isCard={isInSearch} isFocused={isInSearch}>
                <CompactEntityWrapper $isClosed={isClosed}>
                    <TagStyleEntity urn={urn} />
                </CompactEntityWrapper>
                <TabsContainer>
                    <EntitySidebarTabs
                        tabs={tabs}
                        selectedTab={selectedTab}
                        onSelectTab={(name) => setSelectedTabName(name)}
                    />
                </TabsContainer>
            </StyledSidebar>
        </StyledEntitySidebarContainer>
    );
}

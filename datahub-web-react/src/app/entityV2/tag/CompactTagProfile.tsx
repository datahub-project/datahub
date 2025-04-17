import { BookOpen } from '@phosphor-icons/react';
import React, { useContext, useState } from 'react';
import { matchPath, useLocation } from 'react-router';
import styled from 'styled-components';

import {
    StyledEntitySidebarContainer,
    StyledSidebar,
} from '@app/entityV2/shared/containers/profile/sidebar/EntityProfileSidebar';
import EntitySidebarSectionsTab from '@app/entityV2/shared/containers/profile/sidebar/EntitySidebarSectionsTab';
import { EntitySidebarTabs } from '@app/entityV2/shared/containers/profile/sidebar/EntitySidebarTabs';
import { defaultTabDisplayConfig } from '@app/entityV2/shared/containers/profile/utils';
import { EntitySidebarTab } from '@app/entityV2/shared/types';
import TagStyleEntity from '@app/shared/TagStyleEntity';
import EntitySidebarContext from '@app/sharedV2/EntitySidebarContext';
import { PageRoutes } from '@conf/Global';

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
            icon: BookOpen,
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

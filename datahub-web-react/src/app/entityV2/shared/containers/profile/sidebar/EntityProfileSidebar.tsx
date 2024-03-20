/* eslint-disable prefer-template */
import React, { useContext, useEffect, useState } from 'react';
import styled from 'styled-components';
import EntitySidebarContext from '../../../../../shared/EntitySidebarContext';
import { SEARCH_COLORS } from '../../../constants';
import { EntitySidebarTab, TabContextType, TabRenderType } from '../../../types';
import { EntitySidebarTabs } from './EntitySidebarTabs';
import SidebarCollapsibleHeader from './SidebarCollapsibleHeader';
import { EntityMenuItems } from '../../../EntityDropdown/EntityMenuActions';

export const StyledEntitySidebarContainer = styled.div<{
    isCollapsed: boolean;
    $width?: number;
    backgroundColor?: string;
    isCard: boolean;
    isFocused?: boolean;
}>`
    flex: 1;
    overflow: auto;

    ${(props) => !props.isCollapsed && props.$width && `min-width: ${props.$width}px; max-width: ${props.$width}px;`}
    ${(props) => props.isCollapsed && 'min-width: 56px; max-width: 56px;'}
    ${(props) => props.backgroundColor && `background-color: ${props.backgroundColor};`}
        /* Hide scrollbar for Chrome, Safari, and Opera */
    &::-webkit-scrollbar {
        display: none;
    }

    margin: ${(props) => (props.isFocused ? '12px 12px 12px 0px' : '0px 0px 0px 0px')};
    transition: max-width 0.3s ease-in-out, min-width 0.3s ease-in-out;
`;

export const StyledSidebar = styled.div<{ isCard: boolean; isFocused?: boolean; isInSearch?: boolean }>`
    background-color: #ffffff;
    box-shadow: ${(props) => (props.isCard ? '0px 0px 5px rgba(0, 0, 0, 0.08)' : 'none')};
    border-radius: ${(props) => (props.isCard || props.isInSearch ? '8px' : 'none')};
    border: none;
    overflow: hidden;
    min-height: 100%;
    display: flex;
    border-top: ${(props) => (props.isFocused ? `1px solid ${SEARCH_COLORS.TITLE_PURPLE}` : 'inherit')};
    border-top-width: ${(props) => (props.isFocused ? 'medium' : 'inherit')};
`;

const Body = styled.div`
    display: flex;
    align-items: space-between;
    justify-content: start;
    min-height: 100%;
    flex: 1;
`;

const Content = styled.div`
    flex: 1;
    min-height: 100%;
    display: flex;
    flex-direction: column;
    overflow: hidden;
    white-space: nowrap;
`;

const ContentContainer = styled.div<{ isVisible: boolean }>`
    flex: 1;
    ${(props) => props.isVisible && 'border-right: 1px solid #e8e8e8;'}
    overflow: inherit;
`;

const TabsContainer = styled.div``;

const Tabs = styled.div``;

interface Props {
    type?: 'card' | 'default';
    focused?: boolean;
    tabs: EntitySidebarTab[];
    backgroundColor?: string;
    contextType?: TabContextType;
    hideCollapse?: boolean;
    width?: number;
    headerDropdownItems?: Set<EntityMenuItems>;
}

export default function EntityProfileSidebar({
    type = 'default',
    focused = false,
    tabs,
    backgroundColor,
    contextType = TabContextType.PROFILE_SIDEBAR,
    hideCollapse = false,
    width,
    headerDropdownItems,
}: Props) {
    const { isClosed, setSidebarClosed } = useContext(EntitySidebarContext);

    // TODO: Allow selecting a tab via the URL.
    const [selectedTabName, setSelectedTabName] = useState(tabs[0].name);
    const selectedTab = tabs.find((tab) => tab.name === selectedTabName);

    const isCardLayout = type === 'card';

    // Open tab when selected tab is changed
    useEffect(() => {
        if (!window.location.pathname.includes('Lineage')) setSidebarClosed(false);
    }, [selectedTabName, setSidebarClosed]);

    return (
        <StyledEntitySidebarContainer
            isCollapsed={isClosed}
            $width={width}
            backgroundColor={backgroundColor}
            id="entity-profile-sidebar"
            isCard={isCardLayout}
            isFocused={focused}
        >
            <StyledSidebar isCard={isCardLayout} isFocused={focused}>
                <ContentContainer isVisible={!isClosed}>
                    {!hideCollapse && (
                        <SidebarCollapsibleHeader currentTab={selectedTab} headerDropdownItems={headerDropdownItems} />
                    )}
                    <Body>
                        {selectedTab && (
                            <Content>
                                <selectedTab.component
                                    properties={selectedTab.properties}
                                    renderType={TabRenderType.COMPACT}
                                    contextType={contextType}
                                />
                            </Content>
                        )}
                    </Body>
                </ContentContainer>
                <TabsContainer>
                    <Tabs>
                        <EntitySidebarTabs
                            tabs={tabs}
                            selectedTab={selectedTab}
                            onSelectTab={(name) => setSelectedTabName(name)}
                        />
                    </Tabs>
                </TabsContainer>
            </StyledSidebar>
        </StyledEntitySidebarContainer>
    );
}

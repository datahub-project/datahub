/* eslint-disable prefer-template */
import React, { useContext, useState } from 'react';
import styled from 'styled-components';
import EntitySidebarContext from '../../../../../sharedV2/EntitySidebarContext';
import { SEARCH_COLORS } from '../../../constants';
import { EntitySidebarTab, TabContextType, TabRenderType } from '../../../types';
import { EntitySidebarTabs } from './EntitySidebarTabs';
import SidebarCollapsibleHeader from './SidebarCollapsibleHeader';
import { EntityMenuItems } from '../../../EntityDropdown/EntityMenuActions';

export const StyledEntitySidebarContainer = styled.div<{
    isCollapsed: boolean;
    $width?: number;
    backgroundColor?: string;
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

export const StyledSidebar = styled.div<{ isCard: boolean; isFocused?: boolean }>`
    background-color: #ffffff;
    box-shadow: ${(props) => (props.isCard ? '0px 0px 5px rgba(0, 0, 0, 0.08)' : 'none')};
    margin: 2px;
    border-radius: ${(props) => (props.isCard ? '8px' : 'none')};
    border: none;
    overflow: hidden;
    height: 100%;
    display: flex;
    border-top: ${(props) => (props.isFocused ? `1px solid ${SEARCH_COLORS.TITLE_PURPLE}` : 'inherit')};
    border-top-width: ${(props) => (props.isFocused ? 'medium' : 'inherit')};
`;

const Body = styled.div`
    display: flex;
    align-items: space-between;
    justify-content: start;
    flex: 1;
    overflow: auto;
`;

const Content = styled.div`
    flex: 1;
    min-height: 100%;
    display: flex;
    flex-direction: column;
    overflow: auto;
    white-space: nowrap;
    /* hide the scrollbar */

    ::-webkit-scrollbar {
        display: none; /* for Chrome, Safari and Opera */
    }

    -ms-overflow-style: none; /* IE and Edge */
    scrollbar-width: none; /* Firefox */
`;

const ContentContainer = styled.div<{ isVisible: boolean }>`
    flex: 1;
    ${(props) => props.isVisible && 'border-right: 1px solid #e8e8e8;'}
    overflow: auto;
    display: flex;
    flex-direction: column;
    /* hide the scrollbar */

    ::-webkit-scrollbar {
        display: none; /* for Chrome, Safari and Opera */
    }

    -ms-overflow-style: none; /* IE and Edge */
    scrollbar-width: none; /* Firefox */
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
    className?: string;
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
    className,
}: Props) {
    const { isClosed } = useContext(EntitySidebarContext);

    // TODO: Allow selecting a tab via the URL.
    const [selectedTabName, setSelectedTabName] = useState(tabs[0].name);
    const selectedTab = tabs.find((tab) => tab.name === selectedTabName);

    const isCardLayout = type === 'card';

    return (
        <StyledEntitySidebarContainer
            isCollapsed={isClosed}
            $width={width}
            backgroundColor={backgroundColor}
            id="entity-profile-sidebar"
            isFocused={focused}
            className={className}
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
                            hideCollapse={contextType === TabContextType.CHROME_SIDEBAR}
                        />
                    </Tabs>
                </TabsContainer>
            </StyledSidebar>
        </StyledEntitySidebarContainer>
    );
}

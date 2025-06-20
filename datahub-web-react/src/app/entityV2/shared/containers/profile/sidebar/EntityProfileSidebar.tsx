/* eslint-disable prefer-template */
import React, { useContext, useState } from 'react';
import styled from 'styled-components';

import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { EntitySidebarTabs } from '@app/entityV2/shared/containers/profile/sidebar/EntitySidebarTabs';
import SidebarCollapsibleHeader from '@app/entityV2/shared/containers/profile/sidebar/SidebarCollapsibleHeader';
import { EntitySidebarTab, TabContextType, TabRenderType } from '@app/entityV2/shared/types';
import EntitySidebarContext from '@app/sharedV2/EntitySidebarContext';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

export const StyledEntitySidebarContainer = styled.div<{
    isCollapsed: boolean;
    $width?: number;
    backgroundColor?: string;
    isFocused?: boolean;
    $isShowNavBarRedesign?: boolean;
}>`
    flex: 1;
    overflow: auto;
    box-shadow: ${(props) =>
        props.$isShowNavBarRedesign
            ? props.theme.styles['box-shadow-navbar-redesign']
            : '0px 0px 6px 0px rgba(93, 102, 139, 0.2)'};
    ${(props) => !props.isCollapsed && props.$width && `min-width: ${props.$width}px; max-width: ${props.$width}px;`}
    ${(props) => props.isCollapsed && 'min-width: 64px; max-width: 64px;'}
    ${(props) => props.backgroundColor && `background-color: ${props.backgroundColor};`}
    /* Hide scrollbar for Chrome, Safari, and Opera */
    &::-webkit-scrollbar {
        display: none;
    }

    margin: ${(props) => {
        if (props.$isShowNavBarRedesign) {
            return '4px 4px 4px 8px';
        }
        return '0px 0px 0px 0px';
    }};
    ${(props) =>
        props.$isShowNavBarRedesign && `border-radius: ${props.theme.styles['border-radius-navbar-redesign']};`}
    transition: max-width 0.3s ease-in-out, min-width 0.3s ease-in-out;
`;

export const StyledSidebar = styled.div<{ isCard: boolean; isFocused?: boolean; $isShowNavBarRedesign?: boolean }>`
    background-color: #ffffff;
    box-shadow: ${(props) => (props.isCard ? '0px 0px 5px rgba(0, 0, 0, 0.08)' : 'none')};
    border-radius: ${(props) => {
        if (!props.isCard) return 'none';
        return props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px';
    }};
    border: none;
    overflow: hidden;
    height: 100%;
    display: flex;
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
    /* Remove fixed height to allow proper flex behavior */
    /* hide the scrollbar */
    ::-webkit-scrollbar {
        display: none; /* for Chrome, Safari and Opera */
    }
    -ms-overflow-style: none; /* IE and Edge */
    scrollbar-width: none; /* Firefox */
`;

const TabsContainer = styled.div`
    display: flex;
    height: 100%;
    flex-direction: column;
    justify-content: flex-start;
    align-items: center;
`;

const Tabs = styled.div`
    display: flex;
    height: 100%;
    flex-direction: column;
    justify-content: flex-start;
    align-items: center;
`;

interface Props {
    type?: 'card' | 'default';
    focused?: boolean;
    tabs: EntitySidebarTab[];
    backgroundColor?: string;
    contextType?: TabContextType;
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
    width,
    headerDropdownItems,
    className,
}: Props) {
    const { isClosed } = useContext(EntitySidebarContext);
    const isShowNavBarRedesign = useShowNavBarRedesign();

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
            $isShowNavBarRedesign={isShowNavBarRedesign}
        >
            <StyledSidebar isCard={isCardLayout} isFocused={focused} $isShowNavBarRedesign={isShowNavBarRedesign}>
                <ContentContainer isVisible={!isClosed}>
                    <SidebarCollapsibleHeader currentTab={selectedTab} headerDropdownItems={headerDropdownItems} />
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

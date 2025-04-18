import { Button, colors } from '@components';
import { Divider, Typography } from 'antd';
import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

import { SEARCH_RESULTS_BROWSE_SIDEBAR_ID } from '@app/onboarding/config/SearchOnboardingConfig';
import { useIsPlatformBrowseMode } from '@app/searchV2/sidebar/BrowseContext';
import EntityBrowse from '@app/searchV2/sidebar/EntityBrowse';
import PlatformBrowse from '@app/searchV2/sidebar/PlatformBrowse';
import { ProfileSidebarResizer } from '@src/app/entityV2/shared/containers/profile/sidebar/ProfileSidebarResizer';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

const PLATFORM_BROWSE_TRANSITION_MS = 200;
export const MAX_BROWSER_WIDTH = 500;
export const MIN_BROWSWER_WIDTH = 260;

const StyledEntitySidebarContainer = styled.div<{
    isCollapsed: boolean;
    isHidden: boolean;
    $width?: number;
    backgroundColor?: string;
    $isShowNavBarRedesign?: boolean;
}>`
    flex: 1;
    overflow: hidden;
    display: ${(props) => (props.isHidden ? 'none' : undefined)};

    ${(props) => !props.isCollapsed && props.$width && `max-width: ${props.$width}px;`}
    ${(props) => props.isCollapsed && 'min-width: 63px; max-width: 63px;'}
    &::-webkit-scrollbar {
        display: none;
    }

    margin: ${(props) => {
        if (props.$isShowNavBarRedesign) {
            return props.isCollapsed ? '8px 4px 5px 5px' : '8px 0px 5px 5px';
        }
        return props.isCollapsed ? '12px' : '12px 0 12px 12px';
    }};
    transition:
        max-width ${PLATFORM_BROWSE_TRANSITION_MS}ms ease-in-out,
        min-width ${PLATFORM_BROWSE_TRANSITION_MS}ms ease-in-out;

    background-color: #ffffff;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    box-shadow: ${(props) =>
        props.$isShowNavBarRedesign
            ? props.theme.styles['box-shadow-navbar-redesign']
            : '0px 0px 5px rgba(0, 0, 0, 0.08)'};
`;

export const StyledSidebar = styled.div`
    overflow: auto;
    height: 100%;
    display: flex;
    flex-direction: column;
`;

const Controls = styled.div<{ isCollapsed: boolean }>`
    display: flex;
    align-items: center;
    justify-content: ${(props) => (props.isCollapsed ? 'center' : 'space-between')};
    height: 52px;
    padding: 16px 12px;
    overflow: hidden;
`;

const NavigateTitle = styled(Typography.Title)<{ isClosed: boolean }>`
    && {
        padding: 0;
        margin: 0;
        min-width: 140px;
        display: ${(props) => (props.isClosed ? 'none' : 'block')};
        font-size: 14px;
        line-height: 20px;
        font-weight: bold;
        color: ${colors.gray[1700]};
    }
`;

const ThinDivider = styled(Divider)`
    margin: 0px;
    padding: 0px;
`;

const SidebarBody = styled.div`
    height: calc(100% - 53px);
    overflow: auto;
    white-space: nowrap;
    /* Hide scrollbar altoghether for Chrome, Safari, and Opera */
    &::-webkit-scrollbar {
        display: none;
    }
    scrollbar-gutter: stable;
    -moz-scrollbar-gutter: stable;
    -webkit-scrollbar-gutter: stable;
`;

type Props = {
    visible: boolean;
};

const BrowseSidebar = ({ visible }: Props) => {
    const isPlatformBrowseMode = useIsPlatformBrowseMode();
    const [isClosed, setIsClosed] = useState(false);
    const [isHidden, setIsHidden] = useState(false);
    const isShowNavBarRedesign = useShowNavBarRedesign();

    const [sidebarWidth, setSidebarWidth] = useState(MIN_BROWSWER_WIDTH);

    const hideSidebar = useCallback(() => {
        setIsHidden(true);
        setIsClosed(true);
    }, []);

    const unhideSidebar = useCallback(() => setIsHidden(false), []);

    return (
        <>
            <StyledEntitySidebarContainer
                isCollapsed={isClosed}
                isHidden={isHidden}
                $width={sidebarWidth}
                $isShowNavBarRedesign={isShowNavBarRedesign}
                id="browse-v2"
            >
                <Controls isCollapsed={isClosed}>
                    {!isClosed ? (
                        <NavigateTitle level={5} isClosed={isClosed}>
                            Navigate
                        </NavigateTitle>
                    ) : null}
                    <Button
                        variant="text"
                        color="gray"
                        size="lg"
                        isCircle
                        icon={{ icon: isClosed ? 'ArrowLineRight' : 'ArrowLineLeft', source: 'phosphor' }}
                        isActive={!isClosed}
                        onClick={() => setIsClosed(!isClosed)}
                    />
                </Controls>
                <StyledSidebar id={SEARCH_RESULTS_BROWSE_SIDEBAR_ID}>
                    <ThinDivider />
                    <SidebarBody>
                        {!isPlatformBrowseMode ? (
                            <EntityBrowse visible={visible} />
                        ) : (
                            <PlatformBrowse
                                collapsed={isClosed}
                                expand={() => setIsClosed(false)}
                                visible={visible}
                                hideSidebar={hideSidebar}
                                unhideSidebar={unhideSidebar}
                            />
                        )}
                    </SidebarBody>
                </StyledSidebar>
            </StyledEntitySidebarContainer>
            {!isClosed && (
                <ProfileSidebarResizer
                    setSidePanelWidth={(widthProp) => {
                        setSidebarWidth(Math.min(Math.max(widthProp, MIN_BROWSWER_WIDTH), MAX_BROWSER_WIDTH));
                    }}
                    initialSize={sidebarWidth}
                    isSidebarOnLeft
                />
            )}
        </>
    );
};

export default BrowseSidebar;

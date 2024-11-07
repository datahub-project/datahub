import React, { useCallback, useState } from 'react';
import styled from 'styled-components';
import { Button, Divider, Typography } from 'antd';
import { Tooltip } from '@components';
import { ProfileSidebarResizer } from '@src/app/entityV2/shared/containers/profile/sidebar/ProfileSidebarResizer';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import EntityBrowse from './EntityBrowse';
import PlatformBrowse from './PlatformBrowse';
import { useIsPlatformBrowseMode } from './BrowseContext';
import { ANTD_GRAY } from '../../entityV2/shared/constants';
import SidebarBackArrow from '../../../images/sidebarBackArrow.svg?react';
import { SEARCH_RESULTS_BROWSE_SIDEBAR_ID } from '../../onboarding/config/SearchOnboardingConfig';

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
    transition: max-width ${PLATFORM_BROWSE_TRANSITION_MS}ms ease-in-out,
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
    height: 40px;
    padding: 8px;
    overflow: hidden;
`;

const NavigateTitle = styled(Typography.Title)<{ isClosed: boolean }>`
    && {
        padding: 0px;
        margin: 4px 0px 4px 8px;
        min-width: 140px;
        opacity: ${(props) => (props.isClosed ? '0' : '1')};
    }
`;

const CloseButton = styled(Button)<{ $isActive }>`
    margin: 0px;
    padding: 2px 6px;
    display: flex;
    align-items: center;

    transform: ${(props) => (props.$isActive ? 'translateX(0)' : 'translateX(-75px)')};
    transition: transform ${PLATFORM_BROWSE_TRANSITION_MS}ms ease;

    && {
        color: ${(props) => (props.$isActive ? ANTD_GRAY[9] : ANTD_GRAY[8])};
    }
`;

const ThinDivider = styled(Divider)`
    margin: 0px;
    padding: 0px;
`;

const SidebarBody = styled.div`
    height: calc(100% - 47px);
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

const StyledSidebarBackArrow = styled(SidebarBackArrow)<{ direction: 'left' | 'right' }>`
    cursor: pointer;
    ${(props) => (props.direction === 'right' && 'transform: scaleX(-1);') || undefined}
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
                    <NavigateTitle level={5} isClosed={isClosed}>
                        Navigate
                    </NavigateTitle>
                    <Tooltip
                        placement="left"
                        showArrow={false}
                        title={!isClosed ? 'Close navigator' : 'Open navigator'}
                        mouseEnterDelay={0.7}
                        mouseLeaveDelay={0}
                    >
                        <CloseButton $isActive={!isClosed} type="link" onClick={() => setIsClosed(!isClosed)}>
                            <StyledSidebarBackArrow direction={isClosed ? 'left' : 'right'} />
                        </CloseButton>
                    </Tooltip>
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

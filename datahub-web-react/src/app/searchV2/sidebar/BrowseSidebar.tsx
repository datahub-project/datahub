import React, { useCallback, useState } from 'react';
import styled from 'styled-components';
import { Button, Divider, Tooltip, Typography } from 'antd';
import EntityBrowse from './EntityBrowse';
import PlatformBrowse from './PlatformBrowse';
import { useIsPlatformBrowseMode } from './BrowseContext';
import { ANTD_GRAY } from '../../entityV2/shared/constants';
import SidebarBackArrow from '../../../images/sidebarBackArrow.svg?react';
import { SEARCH_RESULTS_BROWSE_SIDEBAR_ID } from '../../onboarding/config/SearchOnboardingConfig';

export const StyledEntitySidebarContainer = styled.div<{
    isCollapsed: boolean;
    $width?: number;
    backgroundColor?: string;
}>`
    flex: 1;
    overflow: auto;
    ${(props) => !props.isCollapsed && props.$width && `min-width: ${props.$width}px; max-width: ${props.$width}px;`}
    ${(props) => props.isCollapsed && 'min-width: 74px; max-width: 74px;'}
    /* Hide scrollbar for Chrome, Safari, and Opera */
    &::-webkit-scrollbar {
        display: none;
    }
    margin: 12px 0px 12px 0px;
    transition: max-width 0.2s ease-in-out, min-width 0.2s ease-in-out;
`;

export const StyledSidebar = styled.div`
    background-color: #ffffff;
    box-shadow: 0px 0px 5px rgba(0, 0, 0, 0.08);
    border-radius: 8px;
    border: none;
    overflow: hidden;
    min-height: 100%;
    display: flex;
    flex-direction: column;
    margin: 0px 0px 0px 12px;
`;

const Controls = styled.div<{ isCollapsed: boolean }>`
    display: flex;
    align-items: center;
    justify-content: ${(props) => (props.isCollapsed ? 'center' : 'space-between')};
    height: 40px;
    padding: 8px;
    overflow: hidden;
`;

const NavigateTitle = styled(Typography.Title)`
    && {
        padding: 0px;
        margin: 0px;
        margin: 4px 0px 4px 8px;
        min-width: 140px;
    }
`;

const CloseButton = styled(Button)<{ isActive }>`
    margin: 0px;
    padding: 2px 6px;
    display: flex;
    align-items: center;
    && {
        color: ${(props) => (props.isActive ? ANTD_GRAY[9] : ANTD_GRAY[8])};
    }
`;

const ThinDivider = styled(Divider)`
    margin: 0px;
    padding: 0px;
`;

const SidebarBody = styled.div`
    height: calc(100% - 47px);
    padding-left: 8px;
    padding-right: 8px;
    overflow: auto;
    white-space: nowrap;
`;

const StyledSidebarBackArrow = styled(SidebarBackArrow)<{ direction: 'left' | 'right' }>`
    cursor: pointer;
    ${(props) => (props.direction === 'right' && 'transform: scaleX(-1);') || undefined}
`;

type Props = {
    visible: boolean;
    width: number;
};

const BrowseSidebar = ({ visible, width }: Props) => {
    const isPlatformBrowseMode = useIsPlatformBrowseMode();
    const [isClosed, setIsClosed] = useState(false);

    const closeSidebar = useCallback(() => {
        setIsClosed(true);
    }, []);

    return (
        <StyledEntitySidebarContainer isCollapsed={isClosed} $width={width} id="browse-v2">
            <StyledSidebar id={SEARCH_RESULTS_BROWSE_SIDEBAR_ID}>
                <Controls isCollapsed={isClosed}>
                    {!isClosed && <NavigateTitle level={5}>Navigate</NavigateTitle>}
                    <Tooltip
                        placement="left"
                        showArrow={false}
                        title={!isClosed ? 'Close navigator' : 'Open navigator'}
                    >
                        <CloseButton isActive={!isClosed} type="link" onClick={() => setIsClosed(!isClosed)}>
                            <StyledSidebarBackArrow direction={isClosed ? 'left' : 'right'} />
                        </CloseButton>
                    </Tooltip>
                </Controls>
                <ThinDivider />
                <SidebarBody>
                    {!isPlatformBrowseMode ? (
                        <EntityBrowse visible={visible} />
                    ) : (
                        <PlatformBrowse
                            collapsed={isClosed}
                            expand={() => setIsClosed(false)}
                            visible={visible}
                            closeSidebar={closeSidebar}
                        />
                    )}
                </SidebarBody>
            </StyledSidebar>
        </StyledEntitySidebarContainer>
    );
};

export default BrowseSidebar;

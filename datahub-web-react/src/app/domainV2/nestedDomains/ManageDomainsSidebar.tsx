import React, { useCallback, useState } from 'react';
import { Button, Divider } from 'antd';
import { Tooltip } from '@components';
import styled from 'styled-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import useSidebarWidth from '../../sharedV2/sidebar/useSidebarWidth';
import DomainsSidebarHeader from './DomainsSidebarHeader';
import DomainNavigator from './domainNavigator/DomainNavigator';
import DomainSearch from '../DomainSearch';
import { ANTD_GRAY } from '../../entity/shared/constants';
import SidebarBackArrow from '../../../images/sidebarBackArrow.svg?react';

const PLATFORM_BROWSE_TRANSITION_MS = 300;

// TODO: Clean up how we do expand / collapse
const StyledEntitySidebarContainer = styled.div<{
    isCollapsed: boolean;
    $width?: number;
    backgroundColor?: string;
    $isShowNavBarRedesign?: boolean;
    $isEntityProfile?: boolean;
}>`
    flex-shrink: 0;
    max-height: 100%;

    width: ${(props) => (props.isCollapsed ? '63px' : `${props.$width}px`)};
    margin-bottom: ${(props) => (props.$isShowNavBarRedesign ? '0' : '12px')};
    transition: width ${PLATFORM_BROWSE_TRANSITION_MS}ms ease-in-out;

    background-color: #ffffff;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    display: flex;
    flex-direction: column;
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        margin: ${props.$isEntityProfile ? '5px 12px 5px 5px' : '0 16px 0 0'};
        box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};
    `}
`;

const Controls = styled.div<{ isCollapsed: boolean }>`
    display: flex;
    align-items: center;
    justify-content: ${(props) => (props.isCollapsed ? 'center' : 'space-between')};
    padding: 15px 16px 10px 12px;
    overflow: hidden;
    height: 50px;
`;

const CloseButton = styled(Button)<{ $isActive }>`
    margin: 0px;
    padding: 2px 0px;
    display: flex;
    align-items: center;
    transition: transform ${PLATFORM_BROWSE_TRANSITION_MS}ms ease;
    && {
        color: ${(props) => (props.$isActive ? ANTD_GRAY[9] : '#8088a3')};
    }
`;

const ThinDivider = styled(Divider)`
    margin: 0px;
    padding: 0px;
`;

const StyledSidebarBackArrow = styled(SidebarBackArrow)<{ direction: 'left' | 'right' }>`
    cursor: pointer;
    ${(props) => (props.direction === 'right' && 'transform: scaleX(-1);') || undefined}
`;

const StyledSidebar = styled.div`
    overflow: auto;
    height: 100%;
    display: flex;
    flex-direction: column;
`;

type Props = {
    isEntityProfile?: boolean;
};

export default function ManageDomainsSidebarV2({ isEntityProfile }: Props) {
    const width = useSidebarWidth(0.2);
    const [isClosed, setIsClosed] = useState(false);
    const isShowNavBarRedesign = useShowNavBarRedesign();

    const unhideSidebar = useCallback(() => {
        setIsClosed(false);
    }, []);

    return (
        <StyledEntitySidebarContainer
            isCollapsed={isClosed}
            $width={width}
            id="browse-v2"
            $isShowNavBarRedesign={isShowNavBarRedesign}
            $isEntityProfile={isEntityProfile}
        >
            <Controls isCollapsed={isClosed}>
                {!isClosed && <DomainsSidebarHeader />}
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
            <ThinDivider />
            <StyledSidebar>
                <DomainSearch isCollapsed={isClosed} unhideSidebar={unhideSidebar} />
                <ThinDivider />
                <DomainNavigator isCollapsed={isClosed} unhideSidebar={unhideSidebar} />
            </StyledSidebar>
        </StyledEntitySidebarContainer>
    );
}

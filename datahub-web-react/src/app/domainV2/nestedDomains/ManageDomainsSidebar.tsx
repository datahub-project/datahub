import { Button } from '@components';
import { Divider } from 'antd';
import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

import DomainSearch from '@app/domainV2/DomainSearch';
import DomainsSidebarHeader from '@app/domainV2/nestedDomains/DomainsSidebarHeader';
import DomainNavigator from '@app/domainV2/nestedDomains/domainNavigator/DomainNavigator';
import useSidebarWidth from '@app/sharedV2/sidebar/useSidebarWidth';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

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
    padding: 12px;
    overflow: hidden;
    height: 50px;
`;

const ThinDivider = styled(Divider)`
    margin: 0px;
    padding: 0px;
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
            <ThinDivider />
            <StyledSidebar>
                <DomainSearch isCollapsed={isClosed} unhideSidebar={unhideSidebar} />
                <ThinDivider />
                <DomainNavigator isCollapsed={isClosed} unhideSidebar={unhideSidebar} />
            </StyledSidebar>
        </StyledEntitySidebarContainer>
    );
}

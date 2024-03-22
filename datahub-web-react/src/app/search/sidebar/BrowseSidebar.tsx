import React, { useState } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import EntityBrowse from './EntityBrowse';
import { SEARCH_RESULTS_BROWSE_SIDEBAR_ID } from '../../onboarding/config/SearchOnboardingConfig';
import { ANTD_GRAY_V2 } from '../../entity/shared/constants';
import { ProfileSidebarResizer } from '../../entity/shared/containers/profile/sidebar/ProfileSidebarResizer';
import PlatformBrowse from './PlatformBrowse';
import { useIsPlatformBrowseMode } from './BrowseContext';

export const MAX_BROWSER_WIDTH = 500;
export const MIN_BROWSWER_WIDTH = 200;

export const SidebarWrapper = styled.div<{ visible: boolean; width: number }>`
    height: 100%;
    width: ${(props) => (props.visible ? `${props.width}px` : '0')};
    min-width: ${(props) => (props.visible ? `${props.width}px` : '0')};
    transition: width 250ms ease-in-out;
    background-color: ${ANTD_GRAY_V2[1]};
    background: white;
`;

const SidebarHeader = styled.div`
    display: flex;
    align-items: center;
    padding-left: 24px;
    height: 47px;
    border-bottom: 1px solid ${(props) => props.theme.styles['border-color-base']};
    white-space: nowrap;
`;

const SidebarBody = styled.div`
    height: calc(100% - 47px);
    padding-left: 8px;
    padding-right: 8px;
    overflow: auto;
    white-space: nowrap;
`;

const SidebarHeaderTitle = styled(Typography.Title)`
    && {
        margin: 0px;
        padding: 0px;
    }
`;

type Props = {
    visible: boolean;
};

const BrowseSidebar = ({ visible }: Props) => {
    const isPlatformBrowseMode = useIsPlatformBrowseMode();
    const [browserWidth, setBrowserWith] = useState(window.innerWidth * 0.2);
    return (
        <>
            <Sidebar visible={visible} width={browserWidth} id={SEARCH_RESULTS_BROWSE_SIDEBAR_ID} data-testid="browse-v2">
                <SidebarHeader>
                    <SidebarHeaderTitle level={5}>Navigate</SidebarHeaderTitle>
                </SidebarHeader>
                <SidebarBody>
                    {!isPlatformBrowseMode ? <EntityBrowse visible={visible} /> : <PlatformBrowse visible={visible} />}
                </SidebarBody>
            </Sidebar>
            <ProfileSidebarResizer
                setSidePanelWidth={(widthProp) =>
                    setBrowserWith(Math.min(Math.max(widthProp, MIN_BROWSWER_WIDTH), MAX_BROWSER_WIDTH))
                }
                initialSize={browserWidth}
                isSidebarOnLeft
            />
        </>
    );
};

export default BrowseSidebar;

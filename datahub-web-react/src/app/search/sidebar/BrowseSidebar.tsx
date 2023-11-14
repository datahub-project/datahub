import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import EntityBrowse from './EntityBrowse';
import { SEARCH_RESULTS_BROWSE_SIDEBAR_ID } from '../../onboarding/config/SearchOnboardingConfig';
import { ANTD_GRAY_V2 } from '../../entity/shared/constants';
import PlatformBrowse from './PlatformBrowse';
import { useIsPlatformBrowseMode } from './BrowseContext';

const Sidebar = styled.div<{ visible: boolean; width: number }>`
    height: 100%;
    width: ${(props) => (props.visible ? `${props.width}px` : '0')};
    transition: width 250ms ease-in-out;
    border-right: 1px solid ${(props) => props.theme.styles['border-color-base']};
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

const SidebarBody = styled.div<{ visible: boolean }>`
    height: calc(100% - 47px);
<<<<<<< HEAD
    padding-left: 16px;
    padding-right: 12px;
    padding-bottom: 200px;
    overflow: ${(props) => (props.visible ? 'auto' : 'hidden')};
=======
    padding-left: 8px;
    padding-right: 8px;
    overflow: auto;
>>>>>>> 2bf498a1d37... feat(): important: Add platform-based browse behind a feature flag
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
    width: number;
};

const BrowseSidebar = ({ visible, width }: Props) => {
    const isPlatformBrowseMode = useIsPlatformBrowseMode();
    return (
        <Sidebar visible={visible} width={width} id={SEARCH_RESULTS_BROWSE_SIDEBAR_ID} data-testid="browse-v2">
            <SidebarHeader>
                <SidebarHeaderTitle level={5}>Navigate</SidebarHeaderTitle>
            </SidebarHeader>
<<<<<<< HEAD
            <SidebarBody visible={visible}>
                {entityAggregations && !entityAggregations.length && <div>No results found</div>}
                {entityAggregations?.map((entityAggregation) => (
                    <BrowseProvider key={entityAggregation.value} entityAggregation={entityAggregation}>
                        <EntityNode />
                    </BrowseProvider>
                ))}
                {error && <SidebarLoadingError onClickRetry={retry} />}
=======
            <SidebarBody>
                {!isPlatformBrowseMode ? <EntityBrowse visible={visible} /> : <PlatformBrowse visible={visible} />}
>>>>>>> 2bf498a1d37... feat(): important: Add platform-based browse behind a feature flag
            </SidebarBody>
        </Sidebar>
    );
};

export default BrowseSidebar;

import React, { useState } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import EntityNode from './EntityNode';
import { BrowseProvider } from './BrowseContext';
import SidebarLoadingError from './SidebarLoadingError';
import { SEARCH_RESULTS_BROWSE_SIDEBAR_ID } from '../../onboarding/config/SearchOnboardingConfig';
import useSidebarEntities from './useSidebarEntities';
import { ANTD_GRAY_V2 } from '../../entity/shared/constants';
import { ProfileSidebarResizer } from '../../entity/shared/containers/profile/sidebar/ProfileSidebarResizer';

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

const SidebarBody = styled.div<{ visible: boolean }>`
    height: calc(100% - 47px);
    padding-left: 16px;
    padding-right: 12px;
    padding-bottom: 200px;
    overflow: ${(props) => (props.visible ? 'auto' : 'hidden')};
    white-space: nowrap;
`;

type Props = {
    visible: boolean;
};

const BrowseSidebar = ({ visible }: Props) => {
    const { error, entityAggregations, retry } = useSidebarEntities({
        skip: !visible,
    });
    const [browserWidth, setBrowserWith] = useState(window.innerWidth * 0.2);

    return (
        <>
            <SidebarWrapper
                visible={visible}
                width={browserWidth}
                id={SEARCH_RESULTS_BROWSE_SIDEBAR_ID}
                data-testid="browse-v2"
            >
                <SidebarHeader>
                    <Typography.Text strong>Navigate</Typography.Text>
                </SidebarHeader>
                <SidebarBody visible={visible}>
                    {entityAggregations && !entityAggregations.length && <div>No results found</div>}
                    {entityAggregations
                        ?.filter((entityAggregation) => entityAggregation?.value !== 'DATA_PRODUCT')
                        ?.map((entityAggregation) => (
                            <BrowseProvider key={entityAggregation?.value} entityAggregation={entityAggregation}>
                                <EntityNode />
                            </BrowseProvider>
                        ))}
                    {error && <SidebarLoadingError onClickRetry={retry} />}
                </SidebarBody>
            </SidebarWrapper>
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

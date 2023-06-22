import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import EntityNode from './EntityNode';
import { BrowseProvider } from './BrowseContext';
import useAggregationsQuery from './useAggregationsQuery';
import { ENTITY_FILTER_NAME } from '../utils/constants';
import SidebarLoadingError from './SidebarLoadingError';
import { SEARCH_RESULTS_BROWSE_SIDEBAR_ID } from '../../onboarding/config/SearchOnboardingConfig';

const Sidebar = styled.div<{ visible: boolean; width: number }>`
    height: 100%;
    width: ${(props) => (props.visible ? `${props.width}px` : '0')};
    transition: width 250ms ease-in-out;
    border-right: 1px solid ${(props) => props.theme.styles['border-color-base']};
    background-color: #f8f9fa;
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
    padding-left: 16px;
    padding-right: 12px;
    padding-bottom: 200px;
    overflow: auto;
    white-space: nowrap;
`;

type Props = {
    visible: boolean;
    width: number;
};

const BrowseSidebar = ({ visible, width }: Props) => {
    const { error, entityAggregations } = useAggregationsQuery({
        skip: !visible,
        facets: [ENTITY_FILTER_NAME],
    });

    return (
        <Sidebar visible={visible} width={width} id={SEARCH_RESULTS_BROWSE_SIDEBAR_ID} data-testid="browse-v2">
            <SidebarHeader>
                <Typography.Text strong>Navigate</Typography.Text>
            </SidebarHeader>
            <SidebarBody>
                {entityAggregations && !entityAggregations.length && <div>No results found</div>}
                {entityAggregations?.map((entityAggregation) => (
                    <BrowseProvider key={entityAggregation.value} entityAggregation={entityAggregation}>
                        <EntityNode />
                    </BrowseProvider>
                ))}
                {error && <SidebarLoadingError />}
            </SidebarBody>
        </Sidebar>
    );
};

export default BrowseSidebar;

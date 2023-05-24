import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { FacetMetadata } from '../../../types.generated';
import EntityNode from './EntityNode';
import useBrowseV2EnabledEntities from './useBrowseV2EnabledEntities';

const Sidebar = styled.div<{ visible: boolean; width: number }>`
    height: 100%;
    width: ${(props) => (props.visible ? `${props.width}px` : '0')};
    transition: width 0.2s ease-in-out;
    border-right: 1px solid ${(props) => props.theme.styles['border-color-base']};
    background-color: ${ANTD_GRAY[2]};
`;

const SidebarHeader = styled.div`
    display: flex;
    align-items: center;
    padding-left: 24px;
    height: 47px;
    border-bottom: 1px solid ${(props) => props.theme.styles['border-color-base']};
`;

const SidebarBody = styled.div`
    padding-left: 24px;
    padding-right: 12px;
`;

type Props = {
    facets?: Array<FacetMetadata> | null;
    visible: boolean;
    width: number;
};

const BrowseSidebar = ({ facets, visible, width }: Props) => {
    const entityAggregations = useBrowseV2EnabledEntities(facets);

    return (
        <Sidebar visible={visible} width={width}>
            <SidebarHeader>
                <Typography.Text strong>Navigate</Typography.Text>
            </SidebarHeader>
            <SidebarBody>
                {entityAggregations && !entityAggregations.length && <div>No results found</div>}
                {entityAggregations &&
                    entityAggregations.map((entityAggregation) => (
                        <EntityNode key={entityAggregation.value} entityAggregation={entityAggregation} />
                    ))}
            </SidebarBody>
        </Sidebar>
    );
};

export default BrowseSidebar;

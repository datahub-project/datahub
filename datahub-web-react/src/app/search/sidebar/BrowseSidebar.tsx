import React, { memo } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { LoadingOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { EntityType, FacetMetadata } from '../../../types.generated';
import EntityNode from './EntityNode';
import { ENTITY_FILTER_NAME } from '../utils/constants';

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
    white-space: nowrap;
`;

const SidebarBody = styled.div`
    padding-left: 24px;
    padding-right: 12px;
    white-space: nowrap;
`;

type Props = {
    facets?: Array<FacetMetadata> | null;
    loading: boolean;
    visible: boolean;
    width: number;
};

const BrowseSidebar = ({ facets, loading, visible, width }: Props) => {
    const entities =
        facets
            ?.find((facet) => facet.field === ENTITY_FILTER_NAME)
            ?.aggregations.filter((entity) => entity.count > 0) ?? [];

    return (
        <Sidebar visible={visible} width={width}>
            <SidebarHeader>
                <Typography.Text strong>Navigate</Typography.Text>
            </SidebarHeader>
            <SidebarBody>
                {loading ? <LoadingOutlined /> : !entities.length && <div>No results found</div>}
                {entities.map((entity) => (
                    <EntityNode key={entity.value} entityType={entity.value as EntityType} count={entity.count} />
                ))}
            </SidebarBody>
        </Sidebar>
    );
};

export default memo(BrowseSidebar);

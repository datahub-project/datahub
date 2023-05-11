import React, { memo, useCallback, useMemo, useState } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { EntityType, FacetMetadata } from '../../../types.generated';
import { ENTITY_FILTER_NAME } from '../utils/constants';
import EntityNode from './EntityNode';

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
    visible: boolean;
    width: number;
};

const BrowseSidebar = ({ facets, visible, width }: Props) => {
    const entities = useMemo(
        () =>
            facets
                ?.find((facet) => facet.field === ENTITY_FILTER_NAME)
                ?.aggregations.filter((entity) => entity.count > 0) ?? [],
        [facets],
    );

    const [selected, setSelected] = useState<EntityType | null>(null);

    const onSelect = useCallback(
        (entityType: EntityType) => setSelected((current) => (current === entityType ? null : entityType)),
        [],
    );
    return (
        <Sidebar visible={visible} width={width}>
            <SidebarHeader>
                <Typography.Text strong>Navigate</Typography.Text>
            </SidebarHeader>
            <SidebarBody>
                {entities.length ? (
                    entities.map((entity) => (
                        <EntityNode
                            key={entity.value}
                            entityType={entity.value as EntityType}
                            count={entity.count}
                            isSelected={entity.value === selected}
                            onSelect={onSelect}
                        />
                    ))
                ) : (
                    <div>No results found</div>
                )}
            </SidebarBody>
        </Sidebar>
    );
};

export default memo(BrowseSidebar);

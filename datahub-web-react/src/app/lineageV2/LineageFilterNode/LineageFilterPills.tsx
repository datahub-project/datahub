import React, { useContext } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { PlatformAggregate, SubtypeAggregate } from './useFetchFilterNodeContents';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ENTITY_SUB_TYPE_FILTER_NAME, PLATFORM_FILTER_NAME } from '../../searchV2/utils/constants';
import { EntityRegistry } from '../../../entityRegistryContext';
import { getFilterIconAndLabel } from '../../searchV2/filters/utils';
import { getFilterColor } from '../../searchV2/recommendation/utils';
import { ANTD_GRAY, ANTD_GRAY_V2 } from '../../entityV2/shared/constants';
import { LINEAGE_FILTER_ID_PREFIX, LineageFilter, LineageNodesContext, setDefault } from '../common';
import { OverflowTitle } from '../LineageEntityNode/NodeContents';

const Pill = styled.div<{ clickable: boolean; color: string; selected: boolean }>`
    align-items: center;
    background-color: ${({ selected, color }) => (selected ? `${color}40` : 'white')};
    border: 0.5px solid ${ANTD_GRAY[5]};
    border-radius: 20px;
    color: black;
    display: flex;
    font-size: 10px;
    justify-content: space-between;
    padding: 3px 5px;

    ${({ color, clickable }) => {
        if (clickable) {
            return `
                cursor: pointer;
                :hover {
                    background-color: ${color}50;
                    border-color: ${color};
                }`;
        }
        return '';
    }}
`;

const Contents = styled.div`
    align-items: center;
    color: ${ANTD_GRAY_V2[10]};
    display: flex;
    overflow: hidden;
`;

const PillIcon = styled.span`
    display: inline-flex;
    margin-right: 2px;
`;

const PillLabel = styled(OverflowTitle)`
    flex-shrink: 1;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const PillCount = styled(Typography.Text)`
    background-color: ${ANTD_GRAY[3]};
    border-radius: 12px;
    color: ${(props) => props.color};
    flex-shrink: 0;
    font-size: 8px;
    margin-left: 2px;
    padding: 2px 4px;
`;

interface Props<T> {
    agg: T;
    data: LineageFilter;
}

export function PlatformEntry({ agg, data }: Props<PlatformAggregate>) {
    const entityRegistry = useEntityRegistry();
    return LineageFilterPills(PLATFORM_FILTER_NAME, agg, data, entityRegistry);
}

export function SubtypeEntry({ agg, data }: Props<SubtypeAggregate>) {
    const entityRegistry = useEntityRegistry();
    return LineageFilterPills(ENTITY_SUB_TYPE_FILTER_NAME, agg, data, entityRegistry);
}

function LineageFilterPills(
    filterName: string,
    [filterValue, count, entity]: PlatformAggregate | SubtypeAggregate,
    data: LineageFilter,
    entityRegistry: EntityRegistry,
) {
    const { id, direction, contents, parent } = data;
    const { icon, label } = getFilterIconAndLabel(filterName, filterValue, entityRegistry, entity || null, 12);

    const { nodes, setDisplayVersion } = useContext(LineageNodesContext);
    const parentUrn = id.slice(LINEAGE_FILTER_ID_PREFIX.length + 2); // +2 for the direction section
    const filters = nodes.get(parentUrn)?.filters?.[direction];
    const autoEnabled = contents.length === count;
    const selected = autoEnabled || !!filters?.facetFilters.get(filterName)?.has(filterValue);

    function applyFilter(e: React.MouseEvent<HTMLDivElement, MouseEvent>) {
        e.stopPropagation();
        if (filters?.facetFilters) {
            if (selected) {
                setDefault(filters?.facetFilters, filterName, new Set()).delete(filterValue);
            } else {
                setDefault(filters?.facetFilters, filterName, new Set()).add(filterValue);
            }
            setDisplayVersion(([v]) => [v + 1, [id, parent, ...contents]]);
        }
    }

    return (
        <Pill
            className={`pill-${!autoEnabled}`}
            color={getFilterColor(filterName, filterValue)}
            selected={selected}
            clickable={!autoEnabled}
            onClick={autoEnabled ? undefined : applyFilter}
        >
            <Contents>
                <PillIcon>{icon}</PillIcon>
                <PillLabel title={label} />
            </Contents>
            <PillCount>{count}</PillCount>
        </Pill>
    );
}

import { LineageAssetType } from '@app/lineageV2/types';
import React, { Dispatch, SetStateAction, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { useDebounce } from 'react-use';
import { useUpdateNodeInternals } from 'reactflow';
import { Pagination } from 'antd';
import { Tooltip } from '@components';
import { PartitionOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { EventType } from '../../analytics';
import analytics from '../../analytics/analytics';
import { NUM_COLUMNS_PER_PAGE } from '../constants';
import Column from './Column';
import ColumnSearch from './ColumnSearch';
import { FetchedEntity } from '../../lineage/types';
import { LineageDisplayColumn } from './useDisplayedColumns';
import { LINEAGE_COLORS, REDESIGN_COLORS } from '../../entityV2/shared/constants';
import { LineageNodesContext, onClickPreventSelect, TRANSITION_DURATION_MS } from '../common';

const MainColumnsWrapper = styled.div<{ isGhost: boolean }>`
    align-items: center;
    display: flex;
    flex-direction: column;
    font: 12px 'Roboto Mono', monospace;
    width: 100%;
    padding: 8px 11px;
    opacity: ${({ isGhost }) => (isGhost ? 0.5 : 1)};
`;

const SearchBarWrapper = styled.div`
    align-items: center;
    display: flex;
    gap: 8px;
    margin-bottom: 8px;
    width: 100%;
`;

const FilterLineageIcon = styled(PartitionOutlined)<{ count: number; selected: boolean }>`
    ${({ selected }) => (selected ? `color: ${LINEAGE_COLORS.BLUE_1};` : '')};
    padding-right: 4px;

    :hover {
        color: ${REDESIGN_COLORS.BLUE};
    }

    ::after {
        content: '${({ count }) => count}';
        font-size: 6px;
        margin-left: 2px;
        position: absolute;
    }
`;

// Wrap pagination and stop click propagation so that changing page doesn't cause node to be selected
const ColumnPaginationWrapper = styled.div``;

const ColumnPagination = styled(Pagination)`
    display: flex;
    justify-content: center;
    margin-top: 8px;
    overflow: hidden;
    width: 100%;
`;

const HorizontalDivider = styled.hr<{ margin: number }>`
    margin: ${({ margin }) => margin}px 0;
    opacity: 0.1;
    width: 100%;
`;

interface Props {
    entity: FetchedEntity;
    isGhost: boolean;
    showAllColumns: boolean;
    paginatedColumns: LineageDisplayColumn[];
    highlightedColumns: LineageDisplayColumn[];
    numFiltered: number;
    pageIndex: number;
    setPageIndex: Dispatch<SetStateAction<number>>;
    filterText: string;
    setFilterText: Dispatch<SetStateAction<string>>;
    numColumnsWithLineage: number;
    onlyWithLineage: boolean;
    setOnlyWithLineage: Dispatch<SetStateAction<boolean>>;
}

export default function DelayedColumns(props: Props) {
    // TODO: Only delay some props: contents should be immediate
    const [delayedProps, setDelayedProps] = useState<Props>(props);
    useEffect(() => {
        if (
            delayedProps.paginatedColumns.length > props.paginatedColumns.length ||
            delayedProps.highlightedColumns.length > props.highlightedColumns.length ||
            delayedProps.showAllColumns > props.showAllColumns
        ) {
            // Delay removal of columns to allow for transition to complete
            const timeout = setTimeout(() => setDelayedProps(props), TRANSITION_DURATION_MS);
            return () => clearTimeout(timeout);
        }
        setDelayedProps(props);
        return () => null;
    }, [
        props,
        delayedProps.paginatedColumns.length,
        delayedProps.highlightedColumns.length,
        delayedProps.showAllColumns,
    ]);

    return Columns(delayedProps);
}

function Columns(props: Props) {
    const {
        entity,
        isGhost,
        showAllColumns,
        paginatedColumns,
        highlightedColumns,
        numFiltered,
        pageIndex,
        setPageIndex,
        filterText,
        setFilterText,
        numColumnsWithLineage,
        onlyWithLineage,
        setOnlyWithLineage,
    } = props;

    const updateNodeInternals = useUpdateNodeInternals();
    useEffect(() => {
        updateNodeInternals(entity.urn); // Register new column handle positions with React Flow
    }, [entity.urn, updateNodeInternals, showAllColumns, paginatedColumns, highlightedColumns]);

    const allNeighborsFetched = useComputeAllNeighborsFetched(entity);
    const hasColumnPagination = showAllColumns && numFiltered > NUM_COLUMNS_PER_PAGE;

    useDebounce(
        () => {
            if (filterText) {
                analytics.event({
                    type: EventType.SearchLineageColumnsEvent,
                    entityUrn: entity.urn,
                    entityType: entity.type,
                    searchTextLength: filterText.length,
                });
            }
        },
        1000,
        [filterText],
    );

    const enableDisableColumnsFilter = useCallback(
        (e: React.MouseEvent<HTMLSpanElement, MouseEvent>) => {
            onClickPreventSelect(e);
            analytics.event({
                type: EventType.FilterLineageColumnsEvent,
                action: onlyWithLineage ? 'disable' : 'enable',
                entityUrn: entity.urn,
                entityType: entity.type,
                shownCount: numColumnsWithLineage,
            });
            setOnlyWithLineage((prevOnlyWithLineage) => !prevOnlyWithLineage);
        },
        [onlyWithLineage, setOnlyWithLineage, entity.urn, entity.type, numColumnsWithLineage],
    );

    const columnProps = {
        parentUrn: entity.urn,
        entityType: entity.type,
        allNeighborsFetched,
    };

    return (
        <MainColumnsWrapper isGhost={isGhost}>
            {showAllColumns && (
                <SearchBarWrapper>
                    <ColumnSearch searchText={filterText} setSearchText={setFilterText} />
                    <Tooltip title="Only show columns with lineage" placement="right" mouseEnterDelay={0.5}>
                        <FilterLineageIcon
                            count={numColumnsWithLineage}
                            selected={onlyWithLineage}
                            onClick={enableDisableColumnsFilter}
                        />
                    </Tooltip>
                </SearchBarWrapper>
            )}
            {showAllColumns && paginatedColumns.map((col) => <Column key={col.fieldPath} {...col} {...columnProps} />)}
            {showAllColumns && !!paginatedColumns.length && !!highlightedColumns.length && (
                <HorizontalDivider margin={4} />
            )}
            {highlightedColumns.map((col) => (
                <Column key={col.fieldPath} {...col} {...columnProps} />
            ))}
            {hasColumnPagination && (
                <ColumnPaginationWrapper onClick={(e) => e.stopPropagation()}>
                    <ColumnPagination
                        className="nodrag"
                        current={pageIndex + 1}
                        onChange={(page) => setPageIndex(page - 1)}
                        total={numFiltered}
                        pageSize={NUM_COLUMNS_PER_PAGE}
                        size="small"
                        simple
                        showLessItems
                        showSizeChanger={false}
                    />
                </ColumnPaginationWrapper>
            )}
        </MainColumnsWrapper>
    );
}

function useComputeAllNeighborsFetched(entity: FetchedEntity): boolean {
    const { nodes, dataVersion } = useContext(LineageNodesContext);
    const allNeighborsFetched = useMemo(
        () =>
            [...(entity.upstreamRelationships || []), ...(entity.downstreamRelationships || [])].every(
                (child) => child.entity?.urn && !!nodes.get(child.entity.urn)?.entity,
            ),
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [entity.upstreamChildren, entity.downstreamChildren, nodes, dataVersion],
    );
    useEffect(() => {
        if (allNeighborsFetched) {
            const node = nodes.get(entity.urn);
            node?.entity?.lineageAssets?.forEach((asset) => {
                if (asset.type === LineageAssetType.Column) {
                    // eslint-disable-next-line no-param-reassign
                    asset.lineageCountsFetched = true;
                }
            });
        }
    }, [allNeighborsFetched, nodes, entity.urn]);

    return allNeighborsFetched;
}

import { PartitionOutlined } from '@ant-design/icons';
import { Pagination } from '@components';
import React, { Dispatch, SetStateAction, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { useDebounce } from 'react-use';
import { useUpdateNodeInternals } from 'reactflow';
import styled from 'styled-components';

import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { LINEAGE_COLORS, REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { FetchedEntity } from '@app/lineage/types';
import Column from '@app/lineageV3/LineageEntityNode/Column';
import ColumnSearch from '@app/lineageV3/LineageEntityNode/ColumnSearch';
import { LineageDisplayColumn } from '@app/lineageV3/LineageEntityNode/useDisplayedColumns';
import { LineageNodesContext, TRANSITION_DURATION_MS, onClickPreventSelect } from '@app/lineageV3/common';
import { NUM_COLUMNS_PER_PAGE } from '@app/lineageV3/constants';
import { LineageAssetType } from '@app/lineageV3/types';

const MainColumnsWrapper = styled.div<{ isGhost: boolean }>`
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 8px;

    font:
        12px 'Roboto Mono',
        monospace;
    padding: 4px 8px 8px 8px;
    opacity: ${({ isGhost }) => (isGhost ? 0.5 : 1)};
`;

const SearchBarWrapper = styled.div`
    align-items: center;
    display: flex;
    gap: 8px;
    width: 100%;
`;

const OnlyColumnsWrapper = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 4px;
    width: 100%;
`;

// eslint-disable-next-line @typescript-eslint/no-unused-vars
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
const ColumnPaginationWrapper = styled.div`
    cursor: auto;
    width: 100%;
    overflow: hidden;
`;

const ColumnPagination = styled(Pagination)`
    display: flex;
    justify-content: center;
    overflow: hidden;
    width: 100%;
    margin: 0;
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
    selectedColumn: string | null;
    setSelectedColumn: Dispatch<SetStateAction<string | null>>;
    hoveredColumn: string | null;
    setHoveredColumn: Dispatch<SetStateAction<string | null>>;
}

export default function DelayedColumns(props: Props) {
    // TODO: Only delay some props: contents should be immediate
    const [delayedProps, setDelayedProps] = useState<Props>(props);
    useEffect(() => {
        if (
            delayedProps.highlightedColumns.length +
                (delayedProps.showAllColumns ? delayedProps.paginatedColumns.length : 0) >
            props.highlightedColumns.length + (props.showAllColumns ? props.paginatedColumns.length : 0)
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
        selectedColumn,
        setSelectedColumn,
        hoveredColumn,
        setHoveredColumn,
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

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
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
        selectedColumn,
        setSelectedColumn,
        hoveredColumn,
        setHoveredColumn,
    };
    const handleMouseLeave = useCallback(() => {
        setHoveredColumn(null);
    }, [setHoveredColumn]);

    return (
        <MainColumnsWrapper isGhost={isGhost}>
            {showAllColumns && (
                <SearchBarWrapper>
                    <ColumnSearch searchText={filterText} setSearchText={setFilterText} />
                </SearchBarWrapper>
            )}
            {((showAllColumns && !!paginatedColumns.length) || !!highlightedColumns.length) && (
                <OnlyColumnsWrapper onMouseLeave={handleMouseLeave}>
                    {showAllColumns &&
                        paginatedColumns.map((col) => <Column key={col.fieldPath} {...col} {...columnProps} />)}
                    {showAllColumns && !!paginatedColumns.length && !!highlightedColumns.length && (
                        <HorizontalDivider margin={4} />
                    )}
                    {highlightedColumns.map((col) => (
                        <Column key={col.fieldPath} {...col} {...columnProps} />
                    ))}
                </OnlyColumnsWrapper>
            )}
            {hasColumnPagination && (
                <ColumnPaginationWrapper onClick={(e) => e.stopPropagation()}>
                    <ColumnPagination
                        className="nodrag"
                        currentPage={pageIndex + 1}
                        itemsPerPage={NUM_COLUMNS_PER_PAGE}
                        total={numFiltered}
                        onPageChange={(page) => setPageIndex(page - 1)}
                        showLessItems
                        showSizeChanger={false}
                        size="small"
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

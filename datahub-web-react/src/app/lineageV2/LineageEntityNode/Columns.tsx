import React, { Dispatch, SetStateAction, useEffect, useState } from 'react';
import { useUpdateNodeInternals } from 'reactflow';
import { Pagination, Tooltip } from 'antd';
import { PartitionOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { NUM_COLUMNS_PER_PAGE } from '../constants';
import Column from './Column';
import ColumnSearch from './ColumnSearch';
import { FetchedEntity } from '../../lineage/types';
import { LineageDisplayColumn } from './useDisplayedColumns';
import { LINEAGE_COLORS, REDESIGN_COLORS } from '../../entityV2/shared/constants';
import { onMouseDownCapturePreventSelect, TRANSITION_DURATION_MS } from '../common';

const MainColumnsWrapper = styled.div`
    align-items: center;
    display: flex;
    flex-direction: column;
    font: 12px 'Roboto Mono', monospace;
    width: 100%;
    padding: 8px 11px;
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

const ColumnPagination = styled(Pagination)`
    display: flex;
    justify-content: center;
    margin-top: 8px;
`;

const HorizontalDivider = styled.hr<{ margin: number }>`
    margin: ${({ margin }) => margin}px 0;
    opacity: 0.1;
    width: 100%;
`;

interface Props {
    entity: FetchedEntity;
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
        updateNodeInternals(entity.urn);
    }, [entity.urn, updateNodeInternals, paginatedColumns]);

    const hasColumnPagination = showAllColumns && numFiltered > NUM_COLUMNS_PER_PAGE;

    return (
        <MainColumnsWrapper>
            {showAllColumns && (
                <SearchBarWrapper>
                    <ColumnSearch searchText={filterText} setSearchText={setFilterText} />
                    <Tooltip title="Only show columns with lineage" placement="right" mouseEnterDelay={0.5}>
                        <FilterLineageIcon
                            count={numColumnsWithLineage}
                            selected={onlyWithLineage}
                            onClick={() => setOnlyWithLineage((v) => !v)}
                            onMouseDownCapture={onMouseDownCapturePreventSelect}
                        />
                    </Tooltip>
                </SearchBarWrapper>
            )}
            {showAllColumns && paginatedColumns.map((col) => <Column key={col.fieldPath} {...col} urn={entity.urn} />)}
            {showAllColumns && !!paginatedColumns.length && !!highlightedColumns.length && (
                <HorizontalDivider margin={4} />
            )}
            {highlightedColumns.map((col) => (
                <Column key={col.fieldPath} {...col} urn={entity.urn} />
            ))}
            {hasColumnPagination && (
                <ColumnPagination
                    className="nodrag"
                    current={pageIndex + 1}
                    onChange={(page) => setPageIndex(page - 1)}
                    total={numFiltered}
                    pageSize={NUM_COLUMNS_PER_PAGE}
                    size="small"
                    showLessItems
                    showSizeChanger={false}
                />
            )}
        </MainColumnsWrapper>
    );
}

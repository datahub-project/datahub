import { Group } from '@visx/group';
import { Pagination } from 'antd';
import React, { useContext, useEffect, useState } from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import ColumnNode from '@app/lineage/ColumnNode';
import NodeColumnsHeader from '@app/lineage/NodeColumnsHeader';
import {
    EXPAND_COLLAPSE_COLUMNS_TOGGLE_HEIGHT,
    NUM_COLUMNS_PER_PAGE,
    centerY,
    iconX,
    width,
} from '@app/lineage/constants';
import { EntitySelectParams, NodeData } from '@app/lineage/types';
import { LineageExplorerContext } from '@app/lineage/utils/LineageExplorerContext';
import {
    convertInputFieldsToSchemaFields,
    filterColumns,
    haveDisplayedFieldsChanged,
} from '@app/lineage/utils/columnLineageUtils';
import { getTitleHeight } from '@app/lineage/utils/titleUtils';
import { useResetPageIndexAfterSelect } from '@app/lineage/utils/useResetPageIndexAfterSelect';
import usePrevious from '@app/shared/usePrevious';

const StyledPagination = styled(Pagination)`
    display: flex;
    justify-content: center;
`;

interface Props {
    node: { x: number; y: number; data: Omit<NodeData, 'children'> };
    onHover: (params?: EntitySelectParams) => void;
}

export default function LineageEntityColumns({ node, onHover }: Props) {
    const { expandTitles, collapsedColumnsNodes, setVisibleColumnsByUrn, columnsByUrn, setColumnsByUrn } =
        useContext(LineageExplorerContext);
    const [pageIndex, setPageIndex] = useState(0);
    const [filterText, setFilterText] = useState('');
    const areColumnsCollapsed = !!collapsedColumnsNodes[node?.data?.urn || 'noop'];

    const titleHeight = getTitleHeight(expandTitles ? node.data.expandedName || node.data.name : undefined);

    const fields =
        columnsByUrn[node.data.urn || ''] ||
        node.data.schemaMetadata?.fields ||
        convertInputFieldsToSchemaFields(node.data.inputFields);

    const displayedFields = fields?.slice(
        pageIndex * NUM_COLUMNS_PER_PAGE,
        pageIndex * NUM_COLUMNS_PER_PAGE + NUM_COLUMNS_PER_PAGE,
    );

    useResetPageIndexAfterSelect(node.data.urn || '', fields, setPageIndex);

    const previousFilterText = usePrevious(filterText);
    useEffect(() => {
        if (filterText !== previousFilterText) {
            filterColumns(filterText, node, setColumnsByUrn);
            setPageIndex(0);
        }
    }, [filterText, previousFilterText, node, setColumnsByUrn]);

    const previousDisplayedFields = usePrevious(displayedFields);
    useEffect(() => {
        if (haveDisplayedFieldsChanged(displayedFields, previousDisplayedFields)) {
            setVisibleColumnsByUrn((visibleColumnsByUrn) => ({
                ...visibleColumnsByUrn,
                [node?.data?.urn || 'noop']: new Set(displayedFields?.map((field) => field.fieldPath)),
            }));
        }
    }, [displayedFields, node?.data?.urn, setVisibleColumnsByUrn, previousDisplayedFields]);

    const hasColumnPagination =
        (node.data.schemaMetadata?.fields && node.data.schemaMetadata?.fields?.length > NUM_COLUMNS_PER_PAGE) ||
        (node.data.inputFields?.fields && node.data.inputFields.fields.length > NUM_COLUMNS_PER_PAGE);

    return (
        <>
            <rect x={iconX - 21} y={centerY + 55 + titleHeight} width={width - 2} height="0.25" stroke={ANTD_GRAY[6]} />
            <NodeColumnsHeader node={node} filterText={filterText} setFilterText={setFilterText} />
            {!areColumnsCollapsed && (
                <Group>
                    {displayedFields?.map((field, idx) => (
                        <ColumnNode field={field} index={idx} node={node} titleHeight={titleHeight} onHover={onHover} />
                    ))}
                    {hasColumnPagination && (
                        <foreignObject
                            width={width}
                            height={30}
                            x={iconX - 22}
                            y={
                                centerY +
                                68 +
                                titleHeight +
                                EXPAND_COLLAPSE_COLUMNS_TOGGLE_HEIGHT +
                                NUM_COLUMNS_PER_PAGE * 30
                            }
                            onClick={(e) => e.stopPropagation()}
                        >
                            <StyledPagination
                                current={pageIndex + 1}
                                onChange={(page) => setPageIndex(page - 1)}
                                total={fields.length || 0}
                                pageSize={NUM_COLUMNS_PER_PAGE}
                                size="small"
                                showLessItems
                                showSizeChanger={false}
                            />
                        </foreignObject>
                    )}
                </Group>
            )}
        </>
    );
}

import React, { useContext, useEffect, useState } from 'react';
import { Group } from '@vx/group';
import { Pagination } from 'antd';
import styled from 'styled-components';
import { NodeData, EntitySelectParams } from './types';
import { ANTD_GRAY } from '../entity/shared/constants';
import { getTitleHeight } from './utils/titleUtils';
import { LineageExplorerContext } from './utils/LineageExplorerContext';
import { centerY, EXPAND_COLLAPSE_COLUMNS_TOGGLE_HEIGHT, iconX, NUM_COLUMNS_PER_PAGE, width } from './constants';
import ColumnNode from './ColumnNode';
import NodeColumnsHeader from './NodeColumnsHeader';
import useSortColumnsBySelectedField from './utils/useSortColumnsBySelectedField';

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
    const [haveFieldsBeenUpdated, setHaveFieldsBeenUpdated] = useState(false);
    const areColumnsCollapsed = !!collapsedColumnsNodes[node?.data?.urn || 'noop'];

    const titleHeight = getTitleHeight(expandTitles ? node.data.expandedName || node.data.name : undefined);

    const fields = columnsByUrn[node.data.urn || ''] || node.data.schemaMetadata?.fields;

    const displayedFields = fields?.slice(
        pageIndex * NUM_COLUMNS_PER_PAGE,
        pageIndex * NUM_COLUMNS_PER_PAGE + NUM_COLUMNS_PER_PAGE,
    );

    useSortColumnsBySelectedField(node, setPageIndex, setHaveFieldsBeenUpdated);

    useEffect(() => {
        if (haveFieldsBeenUpdated) {
            setVisibleColumnsByUrn((visibleColumnsByUrn) => ({
                ...visibleColumnsByUrn,
                [node?.data?.urn || 'noop']: new Set(displayedFields?.map((field) => field.fieldPath)),
            }));
            setHaveFieldsBeenUpdated(false);
        }
    }, [pageIndex, displayedFields, node.data.urn, setVisibleColumnsByUrn, haveFieldsBeenUpdated]);

    function updatePageIndex(index: number) {
        setPageIndex(index);
        setHaveFieldsBeenUpdated(true);
    }

    const hasColumnPagination =
        node.data.schemaMetadata?.fields && node.data.schemaMetadata?.fields.length > NUM_COLUMNS_PER_PAGE;

    return (
        <>
            <rect x={iconX - 21} y={centerY + 55 + titleHeight} width={width - 2} height="0.25" stroke={ANTD_GRAY[6]} />
            <NodeColumnsHeader node={node} onHover={onHover} />
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
                                onChange={(page) => updatePageIndex(page - 1)}
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

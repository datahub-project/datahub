import React, { useContext, useEffect, useState } from 'react';
import { NodeProps } from 'reactflow';
import styled from 'styled-components';
import { HomeOutlined } from '@ant-design/icons';
import { LineageDisplayContext, LineageEntity, LineageNodesContext, TRANSITION_DURATION_MS } from '../common';
import NodeContents from './NodeContents';
import { LINEAGE_COLORS } from '../../entityV2/shared/constants';
import useDisplayedColumns from './useDisplayedColumns';

export const LINEAGE_ENTITY_NODE_NAME = 'lineage-entity';
const MAX_NODES_FOR_TRANSITION = 50;

const HomeNodeBubble = styled.div`
    align-items: center;
    background-color: ${LINEAGE_COLORS.PURPLE_3};
    border-radius: 10px;
    color: white;
    display: flex;
    font-size: 10px;
    font-weight: 600;
    height: 22px;
    justify-content: center;
    left: 1px;
    padding: 4px 8px;
    position: absolute;
    top: -26px;
`;

export default function LineageEntityNode(props: NodeProps<LineageEntity>) {
    const { data, selected } = props;
    const { urn, type } = data;
    const { nodes, rootUrn } = useContext(LineageNodesContext);
    const { numNodes, setHoveredNode } = useContext(LineageDisplayContext);
    const entity = nodes.get(urn)?.entity;

    const [showColumns, setShowColumns] = useState(false);
    const [onlyWithLineage, setOnlyWithLineage] = useState(false);
    const [pageIndex, setPageIndex] = useState(0);
    const [filterText, setFilterText] = useState('');

    useEffect(() => {
        setPageIndex(0);
    }, [filterText, onlyWithLineage, setPageIndex]);

    const transitionDuration = numNodes <= MAX_NODES_FOR_TRANSITION ? TRANSITION_DURATION_MS : 0;

    const { paginatedColumns, extraHighlightedColumns, numFilteredColumns, numColumnsWithLineage, numColumnsTotal } =
        useDisplayedColumns({
            urn,
            entity,
            showColumns,
            filterText,
            pageIndex,
            onlyWithLineage,
        });

    return (
        <>
            {urn === rootUrn && (
                <HomeNodeBubble>
                    <HomeOutlined style={{ marginRight: 4 }} />
                    Home
                </HomeNodeBubble>
            )}
            <NodeContents
                {...data}
                urn={urn}
                type={type}
                selected={selected}
                entity={entity}
                transitionDuration={transitionDuration}
                rootUrn={rootUrn}
                setHoveredNode={setHoveredNode}
                showColumns={showColumns}
                setShowColumns={setShowColumns}
                onlyWithLineage={onlyWithLineage}
                setOnlyWithLineage={setOnlyWithLineage}
                pageIndex={pageIndex}
                setPageIndex={setPageIndex}
                filterText={filterText}
                setFilterText={setFilterText}
                paginatedColumns={paginatedColumns}
                extraHighlightedColumns={extraHighlightedColumns}
                numFilteredColumns={numFilteredColumns}
                numColumnsWithLineage={numColumnsWithLineage}
                numColumnsTotal={numColumnsTotal}
            />
        </>
    );
}

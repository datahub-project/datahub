import { HomeOutlined } from '@ant-design/icons';
import React, { useContext, useEffect, useMemo, useState } from 'react';
import { NodeProps } from 'reactflow';
import styled from 'styled-components';
import { LineageDirection } from '../../../types.generated';
import { LINEAGE_COLORS } from '../../entityV2/shared/constants';
import { LineageDisplayContext, LineageEntity, LineageNodesContext, TRANSITION_DURATION_MS } from '../common';
import useSearchAcrossLineage from '../useSearchAcrossLineage';
import NodeContents from './NodeContents';
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
    const { urn, type, entity, id, fetchStatus, isExpanded } = data;
    const { rootUrn } = useContext(LineageNodesContext);
    const { numNodes, setHoveredNode } = useContext(LineageDisplayContext);

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

    const refetch = useRefetchLineage(urn);

    return (
        <>
            {urn === rootUrn && (
                <HomeNodeBubble>
                    <HomeOutlined style={{ marginRight: 4 }} />
                    Home
                </HomeNodeBubble>
            )}
            <NodeContents
                id={id}
                urn={urn}
                type={type}
                selected={selected}
                entity={entity}
                fetchStatus={fetchStatus}
                isExpanded={isExpanded}
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
                refetch={refetch}
            />
        </>
    );
}

function useRefetchLineage(urn: string) {
    const nodeContext = useContext(LineageNodesContext);

    const { fetchLineage: fetchLineageUpstream } = useSearchAcrossLineage(
        urn,
        nodeContext,
        LineageDirection.Upstream,
        true,
        false,
        true,
    );
    const { fetchLineage: fetchLineageDownstream } = useSearchAcrossLineage(
        urn,
        nodeContext,
        LineageDirection.Downstream,
        true,
        false,
        true,
    );

    return useMemo(
        () => ({
            [LineageDirection.Upstream]: () => {
                const timeout = setTimeout(fetchLineageUpstream, 7000);
                return () => clearTimeout(timeout);
            },
            [LineageDirection.Downstream]: () => {
                const timeout = setTimeout(fetchLineageDownstream, 7000);
                return () => clearTimeout(timeout);
            },
        }),
        [fetchLineageUpstream, fetchLineageDownstream],
    );
}

import React, { useContext, useEffect, useMemo, useState } from 'react';
import { NodeProps } from 'reactflow';

import NodeContents from '@app/lineageV3/LineageEntityNode/NodeContents';
import useDisplayedColumns from '@app/lineageV3/LineageEntityNode/useDisplayedColumns';
import LineageVisualizationContext from '@app/lineageV3/LineageVisualizationContext';
import {
    LineageDisplayContext,
    LineageEntity,
    LineageNodesContext,
    TRANSITION_DURATION_MS,
    parseColumnRef,
    useIgnoreSchemaFieldStatus,
} from '@app/lineageV3/common';
import useSearchAcrossLineage from '@app/lineageV3/queries/useSearchAcrossLineage';

import { EntityType, LineageDirection } from '@types';

export const LINEAGE_ENTITY_NODE_NAME = 'lineage-entity';
const MAX_NODES_FOR_TRANSITION = 50;

export default function LineageEntityNode(props: NodeProps<LineageEntity>) {
    const { data, selected, dragging } = props;
    const { urn, type, entity, id, fetchStatus, isExpanded, filters, parentDataJob } = data;
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();
    const { rootUrn, rootType, nodes, adjacencyList } = useContext(LineageNodesContext);
    const {
        selectedColumn,
        hoveredColumn,
        setSelectedColumn,
        setHoveredColumn,
        shownUrns,
        setHoveredNode,
        displayedMenuNode,
        setDisplayedMenuNode,
    } = useContext(LineageDisplayContext);
    const { searchQuery, searchedEntity } = useContext(LineageVisualizationContext);

    const [showColumns, setShowColumns] = useState(false);
    const [onlyWithLineage, setOnlyWithLineage] = useState(false);
    const [pageIndex, setPageIndex] = useState(0);
    const [filterText, setFilterText] = useState('');

    useEffect(() => {
        setPageIndex(0);
    }, [filterText, onlyWithLineage, setPageIndex]);

    const transitionDuration = shownUrns.length <= MAX_NODES_FOR_TRANSITION ? TRANSITION_DURATION_MS : 0;

    const { paginatedColumns, extraHighlightedColumns, numFilteredColumns, numColumnsWithLineage, numColumnsTotal } =
        useDisplayedColumns({
            urn,
            entity,
            showColumns,
            filterText,
            pageIndex,
            onlyWithLineage,
        });

    const refetch = useRefetchLineage(urn, type);

    const [selectedColumnUrn] = selectedColumn ? parseColumnRef(selectedColumn) : [null];
    const [hoveredColumnUrn] = hoveredColumn ? parseColumnRef(hoveredColumn) : [null];

    const hasParentDataJob = parentDataJob ? true : undefined;
    return (
        <NodeContents
            id={id}
            urn={urn}
            type={type}
            selected={selected}
            dragging={dragging}
            isSearchedEntity={searchedEntity === urn}
            entity={entity}
            fetchStatus={fetchStatus}
            isExpanded={isExpanded}
            filters={filters}
            transitionDuration={transitionDuration}
            rootUrn={rootUrn}
            rootType={rootType}
            parentDataJob={parentDataJob}
            searchQuery={searchQuery}
            setHoveredNode={setHoveredNode}
            showColumns={showColumns}
            setShowColumns={setShowColumns}
            onlyWithLineage={onlyWithLineage}
            setOnlyWithLineage={setOnlyWithLineage}
            pageIndex={pageIndex}
            setPageIndex={setPageIndex}
            filterText={filterText}
            setFilterText={setFilterText}
            isMenuDisplayed={displayedMenuNode === urn}
            setDisplayedMenuNode={setDisplayedMenuNode}
            selectedColumn={selectedColumnUrn === urn ? selectedColumn : null}
            setSelectedColumn={setSelectedColumn}
            hoveredColumn={hoveredColumnUrn === urn ? hoveredColumn : null}
            setHoveredColumn={setHoveredColumn}
            paginatedColumns={paginatedColumns}
            extraHighlightedColumns={extraHighlightedColumns}
            numFilteredColumns={numFilteredColumns}
            numColumnsWithLineage={numColumnsWithLineage}
            numColumnsTotal={numColumnsTotal}
            refetch={refetch}
            ignoreSchemaFieldStatus={ignoreSchemaFieldStatus}
            numUpstreams={
                hasParentDataJob &&
                Array.from(adjacencyList[LineageDirection.Upstream].get(urn) || []).filter(
                    (upstream) => nodes.get(upstream)?.parentDataJob !== parentDataJob,
                ).length
            }
            numDownstreams={
                hasParentDataJob &&
                Array.from(adjacencyList[LineageDirection.Downstream].get(urn) || []).filter(
                    (downstream) => nodes.get(downstream)?.parentDataJob !== parentDataJob,
                ).length
            }
        />
    );
}

function useRefetchLineage(urn: string, type: EntityType) {
    const nodeContext = useContext(LineageNodesContext);

    const { fetchLineage: fetchLineageUpstream } = useSearchAcrossLineage(
        urn,
        type,
        nodeContext,
        LineageDirection.Upstream,
        true,
        false,
        true,
    );
    const { fetchLineage: fetchLineageDownstream } = useSearchAcrossLineage(
        urn,
        type,
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

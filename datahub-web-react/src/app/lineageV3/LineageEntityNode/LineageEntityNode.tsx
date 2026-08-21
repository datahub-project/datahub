import React, { useContext, useEffect, useState } from 'react';
import { NodeProps } from 'reactflow';

import NodeContents from '@app/lineageV3/LineageEntityNode/NodeContents';
import useDisplayedColumns from '@app/lineageV3/LineageEntityNode/useDisplayedColumns';
import LineageVisualizationContext from '@app/lineageV3/LineageVisualizationContext';
import {
    LineageDisplayContext,
    LineageEntity,
    LineageNodesContext,
    TRANSITION_DURATION_MS,
    createLineageFilterNodeId,
    mayHideLineage,
    parseColumnRef,
    useIgnoreSchemaFieldStatus,
} from '@app/lineageV3/common';
import useRefetchLineage from '@app/lineageV3/queries/useRefetchLineage';
import { getMemberBoundingBoxUrn } from '@app/lineageV3/useComputeGraph/boundingBoxes/boundingBoxes.utils';

import { LineageDirection } from '@types';

export const LINEAGE_ENTITY_NODE_NAME = 'lineage-entity';
const MAX_NODES_FOR_TRANSITION = 50;

export default function LineageEntityNode(props: NodeProps<LineageEntity>) {
    const { data, selected, dragging } = props;
    const { urn, type, entity, id, fetchStatus, isExpanded, filters, parentDataJob, boundingBoxes } = data;
    // Members render one node per bounding box; the box's urn is encoded in the (qualified) node id.
    // Resolve the boolean here (rather than passing `boundingBoxes`) to avoid re-memoizing NodeContents.
    const parentBoundingBoxUrn = getMemberBoundingBoxUrn(id);
    const isOutputPort = !!boundingBoxes?.find((box) => box.urn === parentBoundingBoxUrn)?.isOutputPort;
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();
    const { rootUrn, rootType, nodes, adjacencyList, collapseColumnsVersion } = useContext(LineageNodesContext);
    const {
        selectedColumn,
        hoveredColumn,
        setSelectedColumn,
        setHoveredColumn,
        shownUrns,
        setHoveredNode,
        displayedMenuNode,
        setDisplayedMenuNode,
        lineageFilters,
    } = useContext(LineageDisplayContext);
    const { searchQuery, searchedEntity } = useContext(LineageVisualizationContext);

    const [showColumns, setShowColumns] = useState(false);
    const [onlyWithLineage, setOnlyWithLineage] = useState(false);
    const [pageIndex, setPageIndex] = useState(0);
    const [filterText, setFilterText] = useState('');

    useEffect(() => {
        setPageIndex(0);
    }, [filterText, onlyWithLineage, setPageIndex]);

    // Collapse columns when the graph is redrawn, so nodes are laid out at their default size
    useEffect(() => {
        setShowColumns(false);
    }, [collapseColumnsVersion]);

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
    // Data flow lineage: members count only the neighbors outside their own data job
    const numUpstreams =
        hasParentDataJob &&
        Array.from(adjacencyList[LineageDirection.Upstream].get(urn) || []).filter(
            (upstream) => nodes.get(upstream)?.parentDataJob !== parentDataJob,
        ).length;
    const numDownstreams =
        hasParentDataJob &&
        Array.from(adjacencyList[LineageDirection.Downstream].get(urn) || []).filter(
            (downstream) => nodes.get(downstream)?.parentDataJob !== parentDataJob,
        ).length;

    // Columns only claim to hide lineage in directions where their node does, and pay for the
    // counts query only then. Passed as booleans to keep `NodeContents` memoized.
    const mayHideLineageIn = (
        direction: LineageDirection,
        numNeighbors: number | undefined,
        numChildren: number | undefined,
    ) =>
        mayHideLineage(
            direction,
            data,
            !!(numNeighbors ?? !!numChildren), // Matches `NodeContents`
            !!lineageFilters.get(createLineageFilterNodeId(urn, direction)),
        );

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
            isOutputPort={isOutputPort}
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
            // A selected column anywhere on the graph takes precedence over a hovered one, as it
            // does when computing column highlights, so don't report a hover alongside a selection
            hoveredColumn={!selectedColumn && hoveredColumnUrn === urn ? hoveredColumn : null}
            setHoveredColumn={setHoveredColumn}
            paginatedColumns={paginatedColumns}
            extraHighlightedColumns={extraHighlightedColumns}
            numFilteredColumns={numFilteredColumns}
            numColumnsWithLineage={numColumnsWithLineage}
            numColumnsTotal={numColumnsTotal}
            refetch={refetch}
            ignoreSchemaFieldStatus={ignoreSchemaFieldStatus}
            numUpstreams={numUpstreams}
            numDownstreams={numDownstreams}
            mayHideUpstreamLineage={mayHideLineageIn(
                LineageDirection.Upstream,
                numUpstreams,
                entity?.numUpstreamChildren,
            )}
            mayHideDownstreamLineage={mayHideLineageIn(
                LineageDirection.Downstream,
                numDownstreams,
                entity?.numDownstreamChildren,
            )}
        />
    );
}

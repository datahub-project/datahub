import React, { useContext, useEffect, useState } from 'react';
import { useHistory, useLocation } from 'react-router-dom';
import ReactFlow, { Background, BackgroundVariant, Edge, EdgeTypes, MiniMap, NodeTypes, useReactFlow } from 'reactflow';
import 'reactflow/dist/style.css';
import styled from 'styled-components';

import { LINEAGE_TABLE_EDGE_NAME, LineageTableEdge } from '@app/lineageV2/LineageEdge/LineageTableEdge';
import TentativeEdge, { TENTATIVE_EDGE_NAME } from '@app/lineageV2/LineageEdge/TentativeEdge';
import LineageEntityNode, { LINEAGE_ENTITY_NODE_NAME } from '@app/lineageV2/LineageEntityNode/LineageEntityNode';
import LineageFilterNodeBasic, {
    LINEAGE_FILTER_NODE_NAME,
} from '@app/lineageV2/LineageFilterNode/LineageFilterNodeBasic';
import LineageTransformationNode, {
    LINEAGE_TRANSFORMATION_NODE_NAME,
} from '@app/lineageV2/LineageTransformationNode/LineageTransformationNode';
import LineageVisualizationContext from '@app/lineageV2/LineageVisualizationContext';
import { LineageVisualizationNode } from '@app/lineageV2/NodeBuilder';
import { LineageDisplayContext, TRANSITION_DURATION_MS } from '@app/lineageV2/common';
import LineageControls from '@app/lineageV2/controls/LineageControls';
import SearchControl from '@app/lineageV2/controls/SearchControl';
import ZoomControls from '@app/lineageV2/controls/ZoomControls';
import { getLineageUrl } from '@app/lineageV2/lineageUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

const StyledReactFlow = styled(ReactFlow)<{ $edgesOnTop: boolean }>`
    .react-flow__node-lineage-entity:not(.dragging) {
        transition: transform ${TRANSITION_DURATION_MS}ms ease-in-out;
    }

    ${({ $edgesOnTop }) =>
        $edgesOnTop &&
        `.react-flow__node-lineage-entity:not(.selected) {
            z-index: -1 !important;
        }`}
`;

const nodeTypes: NodeTypes = {
    [LINEAGE_ENTITY_NODE_NAME]: LineageEntityNode,
    [LINEAGE_TRANSFORMATION_NODE_NAME]: LineageTransformationNode,
    [LINEAGE_FILTER_NODE_NAME]: LineageFilterNodeBasic,
};

const edgeTypes: EdgeTypes = {
    [LINEAGE_TABLE_EDGE_NAME]: LineageTableEdge,
    [TENTATIVE_EDGE_NAME]: TentativeEdge,
};

interface Props {
    initialNodes: LineageVisualizationNode[];
    initialEdges: Edge[];
}

// React flow doesn't always emit events if it re-renders at the same time
const MemoizedLineageVisualization = React.memo(LineageVisualization);
export default MemoizedLineageVisualization;

function LineageVisualization({ initialNodes, initialEdges }: Props) {
    const [searchQuery, setSearchQuery] = useState('');
    const [isFocused, setIsFocused] = useState(false);
    const [searchedEntity, setSearchedEntity] = useState<string | null>(null);
    const { highlightedEdges, setSelectedColumn, setDisplayedMenuNode } = useContext(LineageDisplayContext);
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const location = useLocation();

    useFitView(searchedEntity);
    useHandleKeyboardDeselect(setSelectedColumn);

    return (
        <LineageVisualizationContext.Provider
            value={{ searchQuery, setSearchQuery, searchedEntity, setSearchedEntity, isFocused }}
        >
            <StyledReactFlow
                defaultNodes={initialNodes}
                defaultEdges={initialEdges}
                // Selection change event does not get emitted without timeout
                onPaneClick={() => setTimeout(() => setSelectedColumn(null), 0)}
                onClick={() => setDisplayedMenuNode(null)}
                onNodeDragStart={(_e, node) => {
                    // eslint-disable-next-line no-param-reassign
                    node.data.dragged = true;
                }}
                onNodeDoubleClick={(_e, node) => {
                    if (node.data?.urn && node.data?.type) {
                        history.push(getLineageUrl(node.data.urn, node.data.type, location, entityRegistry));
                    }
                }}
                nodeTypes={nodeTypes}
                edgeTypes={edgeTypes}
                proOptions={{ hideAttribution: true }}
                nodesDraggable
                nodeDragThreshold={3}
                selectNodesOnDrag={false}
                nodesConnectable={false}
                minZoom={0.3}
                maxZoom={5}
                fitView
                fitViewOptions={{ maxZoom: 1, duration: 0 }}
                $edgesOnTop={!!highlightedEdges.size}
                tabIndex={0} // Allow focus for keyboard shortcuts
                onFocus={() => setIsFocused(true)}
                onBlur={() => setIsFocused(false)}
            >
                <Background variant={BackgroundVariant.Lines} />
                <ZoomControls />
                <SearchControl />
                <LineageControls />
                <MiniMap position="bottom-right" ariaLabel={null} pannable zoomable />
            </StyledReactFlow>
        </LineageVisualizationContext.Provider>
    );
}

function useFitView(searchedEntity: string | null) {
    const { fitView } = useReactFlow();

    useEffect(() => {
        if (searchedEntity) {
            fitView({ duration: 600, maxZoom: 1, nodes: [{ id: searchedEntity }] });
        }
    }, [searchedEntity, fitView]);
}

function useHandleKeyboardDeselect(setSelectedColumn: (value: string | null) => void) {
    useEffect(() => {
        function handleKeyPress(e: KeyboardEvent) {
            if (e.key === 'Escape') {
                // Prevents conflicts with React Flow escape handler
                const timeout = setTimeout(() => setSelectedColumn(null), 0);
                return () => clearTimeout(timeout);
            }
            return () => {};
        }

        document.addEventListener('keydown', handleKeyPress);
        return () => {
            document.removeEventListener('keydown', handleKeyPress);
        };
    }, [setSelectedColumn]);
}

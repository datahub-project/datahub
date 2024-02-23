import React, { useContext, useEffect } from 'react';
import ReactFlow, { Edge, NodeTypes, Background, BackgroundVariant, MiniMap, EdgeTypes } from 'reactflow';
import styled from 'styled-components';

import 'reactflow/dist/style.css';
import { LineageDisplayContext, TRANSITION_DURATION_MS } from './common';
import LineageEntityNode, { LINEAGE_ENTITY_NODE_NAME } from './LineageEntityNode/LineageEntityNode';
import LineageTransformationNode, {
    LINEAGE_TRANSFORMATION_NODE_NAME,
} from './LineageTransformationNode/LineageTransformationNode';
import TableauWorkbookNode, { LINEAGE_WORKBOOK_NODE_NAME } from './MinorNodes/TableauWorkbookNode';
import LineageFilterNode, { LINEAGE_FILTER_NODE_NAME } from './LineageFilterNode/LineageFilterNode';
import ZoomControls from './controls/ZoomControls';
import LineageControls from './controls/LineageControls';
import { NodeWithMetadata } from './NodeBuilder';

import { LINEAGE_TABLE_EDGE_NAME, LineageTableEdge } from './LineageEdge/LineageTableEdge';

const StyledReactFlow = styled(ReactFlow)`
    .react-flow__node-lineage-entity:not(.dragging) {
        transition: transform ${TRANSITION_DURATION_MS}ms ease-in-out;
    }
`;

const nodeTypes: NodeTypes = {
    [LINEAGE_ENTITY_NODE_NAME]: LineageEntityNode,
    [LINEAGE_TRANSFORMATION_NODE_NAME]: LineageTransformationNode,
    [LINEAGE_FILTER_NODE_NAME]: LineageFilterNode,
    [LINEAGE_WORKBOOK_NODE_NAME]: TableauWorkbookNode,
};

const edgeTypes: EdgeTypes = {
    [LINEAGE_TABLE_EDGE_NAME]: LineageTableEdge,
};

interface Props {
    initialNodes: NodeWithMetadata[];
    initialEdges: Edge[];
}

export default function LineageVisualization({ initialNodes, initialEdges }: Props) {
    const { setSelectedColumn } = useContext(LineageDisplayContext);

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

    return (
        <StyledReactFlow
            defaultNodes={initialNodes}
            defaultEdges={initialEdges}
            onPaneClick={() => setSelectedColumn(null)}
            nodeTypes={nodeTypes}
            edgeTypes={edgeTypes}
            proOptions={{ hideAttribution: true }}
            nodesDraggable
            nodesConnectable={false}
            minZoom={0.3}
            maxZoom={5}
            fitView
            fitViewOptions={{ maxZoom: 1, duration: 0 }}
        >
            <Background variant={BackgroundVariant.Lines} />
            <ZoomControls />
            <LineageControls />
            <MiniMap position="bottom-right" ariaLabel={null} pannable zoomable />
        </StyledReactFlow>
    );
}

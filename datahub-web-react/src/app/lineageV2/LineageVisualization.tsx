import React, { useContext, useEffect } from 'react';
import ReactFlow, {
    useEdgesState,
    useNodesState,
    Edge,
    NodeTypes,
    Background,
    BackgroundVariant,
    useReactFlow,
    MiniMap,
    EdgeTypes,
} from 'reactflow';
import styled from 'styled-components';

import 'reactflow/dist/style.css';
import { LineageDisplayContext } from './common';
import { TRANSITION_DURATION_MS } from './LineageEntityNode/useDisplayedColumns';
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
    const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
    const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);
    const { getNode, getEdge } = useReactFlow();
    const { setSelectedColumn } = useContext(LineageDisplayContext);

    useEffect(() => {
        const initialNodeMap = new Map(initialNodes.map((node) => [node.id, node]));
        const nodesToAdd = initialNodes.filter((node) => !getNode(node.id));
        const layersToRedraw = new Set<number>(
            nodesToAdd.map((node) => node?.layer).filter((layer): layer is number => !!layer),
        );
        const nodesToRedraw = initialNodes.filter((node) => node.layer && layersToRedraw.has(node.layer));
        setNodes((oldNodes) => [
            ...oldNodes
                .filter((n) => initialNodeMap.has(n.id))
                .map((n) => ({
                    ...n,
                    data: initialNodeMap.get(n.id)?.data || n.data,
                })),
            ...nodesToAdd,
            ...nodesToRedraw,
        ]);
    }, [initialNodes, getNode, setNodes]);

    useEffect(() => {
        const edgesToAdd = initialEdges.filter((edge) => !getEdge(edge.id));
        setEdges((oldEdges) => [...oldEdges, ...edgesToAdd]);
    }, [initialEdges, getEdge, setEdges]);

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
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onPaneClick={() => setSelectedColumn(null)}
            nodeTypes={nodeTypes}
            edgeTypes={edgeTypes}
            proOptions={{ hideAttribution: true }}
            nodesDraggable
            nodesConnectable={false}
            minZoom={0.3}
            maxZoom={5}
            fitView
            fitViewOptions={{ maxZoom: 1 }}
        >
            <Background variant={BackgroundVariant.Lines} />
            <ZoomControls />
            <LineageControls />
            <MiniMap position="bottom-right" ariaLabel={null} pannable zoomable />
        </StyledReactFlow>
    );
}

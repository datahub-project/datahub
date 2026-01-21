import React, { useContext, useEffect, useMemo, useState } from 'react';
import ReactFlow, { Background, BackgroundVariant, Edge, EdgeTypes, MiniMap, NodeTypes, useReactFlow } from 'reactflow';
import 'reactflow/dist/style.css';
import styled from 'styled-components';

import LineageAnnotationNode, {
    LINEAGE_ANNOTATION_NODE,
} from '@app/lineageV3/LineageAnnotationNode/LineageAnnotationNode';
import useAddAnnotationNodes from '@app/lineageV3/LineageAnnotationNode/useAddAnnotationNodes';
import LineageBoundingBoxNode, {
    LINEAGE_BOUNDING_BOX_NODE_NAME,
} from '@app/lineageV3/LineageBoundingBoxNode/LineageBoundingBoxNode';
import { CUSTOM_SMOOTH_STEP_EDGE_NAME, CustomSmoothStepEdge } from '@app/lineageV3/LineageEdge/CustomSmoothStepEdge';
import {
    DATA_JOB_INPUT_OUTPUT_EDGE_NAME,
    DataJobInputOutputEdge,
} from '@app/lineageV3/LineageEdge/DataJobInputOutputEdge';
import { LINEAGE_TABLE_EDGE_NAME, LineageTableEdge } from '@app/lineageV3/LineageEdge/LineageTableEdge';
import TentativeEdge, { TENTATIVE_EDGE_NAME } from '@app/lineageV3/LineageEdge/TentativeEdge';
import LineageEntityNode, { LINEAGE_ENTITY_NODE_NAME } from '@app/lineageV3/LineageEntityNode/LineageEntityNode';
import LineageFilterNodeBasic, {
    LINEAGE_FILTER_NODE_NAME,
} from '@app/lineageV3/LineageFilterNode/LineageFilterNodeBasic';
import LineageGraphContext from '@app/lineageV3/LineageGraphContext';
import LineageTransformationNode, {
    LINEAGE_TRANSFORMATION_NODE_NAME,
} from '@app/lineageV3/LineageTransformationNode/LineageTransformationNode';
import LineageVisualizationContext from '@app/lineageV3/LineageVisualizationContext';
import { LineageDisplayContext, TRANSITION_DURATION_MS } from '@app/lineageV3/common';
import LineageControls from '@app/lineageV3/controls/LineageControls';
import SearchControl from '@app/lineageV3/controls/SearchControl';
import ZoomControls from '@app/lineageV3/controls/ZoomControls';
import LineageSVGs from '@app/lineageV3/lineageSVGs';
import { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';
import { LevelsInfo } from '@app/lineageV3/useComputeGraph/limitNodes/limitNodesUtils';

const StyledReactFlow = styled(ReactFlow)<{ isDraggingBoundingBox: boolean; $edgesOnTop: boolean }>`
    ${({ isDraggingBoundingBox }) =>
        !isDraggingBoundingBox &&
        `.react-flow__node-lineage-entity:not(.dragging) {
            transition: transform ${TRANSITION_DURATION_MS}ms ease-in-out;
        }`}
`;

// TODO: Bring back after figuring out how to no overlap expand / contract actions
// On node hover, bring edges to the top
// ${({ $edgesOnTop }) =>
//     $edgesOnTop &&
//     `.react-flow__node-lineage-entity:not(.selected) {
//         z-index: -1 !important;
//     }`}

const nodeTypes: NodeTypes = {
    [LINEAGE_ENTITY_NODE_NAME]: LineageEntityNode,
    [LINEAGE_TRANSFORMATION_NODE_NAME]: LineageTransformationNode,
    [LINEAGE_FILTER_NODE_NAME]: LineageFilterNodeBasic,
    [LINEAGE_BOUNDING_BOX_NODE_NAME]: LineageBoundingBoxNode,
    [LINEAGE_ANNOTATION_NODE]: LineageAnnotationNode,
};

const edgeTypes: EdgeTypes = {
    [LINEAGE_TABLE_EDGE_NAME]: LineageTableEdge,
    [TENTATIVE_EDGE_NAME]: TentativeEdge,
    [DATA_JOB_INPUT_OUTPUT_EDGE_NAME]: DataJobInputOutputEdge,
    [CUSTOM_SMOOTH_STEP_EDGE_NAME]: CustomSmoothStepEdge,
};

interface Props {
    initialNodes: LineageVisualizationNode[];
    initialEdges: Edge[];
    levelsInfo: LevelsInfo;
    levelsMap: Map<string, number>;
}

// React flow doesn't always emit events if it re-renders at the same time
const MemoizedLineageVisualization = React.memo(LineageVisualization);
export default MemoizedLineageVisualization;

function LineageVisualization({ initialNodes, initialEdges, levelsInfo, levelsMap }: Props) {
    const [searchQuery, setSearchQuery] = useState('');
    const [isFocused, setIsFocused] = useState(false);
    const [isDraggingBoundingBox, setIsDraggingBoundingBox] = useState(false);
    const [searchedEntity, setSearchedEntity] = useState<string | null>(null);
    const { highlightedEdges, setSelectedColumn, setDisplayedMenuNode } = useContext(LineageDisplayContext);
    useFitView(searchedEntity);
    useHandleKeyboardDeselect(setSelectedColumn);
    const { isModuleView } = useContext(LineageGraphContext);
    const addAnnotationNodes = useAddAnnotationNodes();

    const { nodes, edges } = useMemo(() => {
        if (!isModuleView) {
            return {
                nodes: initialNodes,
                edges: initialEdges,
            };
        }

        return addAnnotationNodes(initialNodes, initialEdges, levelsInfo, levelsMap);
    }, [isModuleView, initialNodes, initialEdges, levelsInfo, levelsMap, addAnnotationNodes]);

    return (
        <LineageVisualizationContext.Provider
            value={{
                searchQuery,
                setSearchQuery,
                searchedEntity,
                setSearchedEntity,
                isFocused,
                isDraggingBoundingBox,
                setIsDraggingBoundingBox,
            }}
        >
            <LineageSVGs />
            <StyledReactFlow
                isDraggingBoundingBox={isDraggingBoundingBox}
                defaultNodes={nodes}
                defaultEdges={edges}
                // Selection change event does not get emitted without timeout
                onPaneClick={() => setTimeout(() => setSelectedColumn(null), 0)}
                onClick={() => setDisplayedMenuNode(null)}
                onNodeDragStart={(_e, node) => {
                    // eslint-disable-next-line no-param-reassign
                    node.data.dragged = true;
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
                <Background variant={BackgroundVariant.Dots} />
                {!isModuleView && (
                    <>
                        <ZoomControls />
                        <SearchControl />
                        <LineageControls />
                        <MiniMap position="bottom-right" ariaLabel={null} pannable zoomable />
                    </>
                )}
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

import React, { useCallback, useContext, useState } from 'react';
import { useOnSelectionChange, useStore } from 'reactflow';
import styled from 'styled-components/macro';
import EntitySidebarContext, { EntitySidebarQueryDetails } from '../sharedV2/EntitySidebarContext';
import CompactContext from '../shared/CompactContext';
import useSidebarWidth from '../sharedV2/sidebar/useSidebarWidth';
import { useEntityRegistry } from '../useEntityRegistry';
import { NeighborMap, LineageDisplayContext, LineageEntity, LineageNodesContext } from './common';
import { EntityType } from '../../types.generated';

const SidebarWrapper = styled.div<{ distanceFromTop: number }>`
    position: absolute;
    right: 0;
    top: 0;
    display: flex;
    flex-direction: column;
    z-index: 9999;
    height: 100vh;
    // height: calc(100vh - ${(props) => props.distanceFromTop}px);
    // top: ${(props) => props.distanceFromTop}px;
    // overflow-y: scroll;
    border-left: 1px solid #e8e8e8;

    && {
        &::-webkit-scrollbar {
            display: none;
        }
    }
`;

export default function LineageSidebar() {
    const entityRegistry = useEntityRegistry();
    const selectedEntity = useSelectedNode();
    const resetSelectedElements = useStore((actions) => actions.resetSelectedElements);
    const queryDetails = useQueryDetails(selectedEntity);
    const width = useSidebarWidth();

    const setSidebarClosed = useCallback(
        (closed) => {
            if (closed) {
                resetSelectedElements();
            }
        },
        [resetSelectedElements],
    );

    // This manages closing, rather than isClosed
    if (!selectedEntity) {
        return null;
    }

    return (
        <EntitySidebarContext.Provider
            value={{
                width,
                isClosed: false,
                setSidebarClosed,
                forLineage: true,
                separateSiblings: true,
                extra: queryDetails,
            }}
        >
            <SidebarWrapper distanceFromTop={0}>
                <CompactContext.Provider key={selectedEntity.urn} value>
                    {entityRegistry.renderProfile(selectedEntity.type, selectedEntity.urn)}
                </CompactContext.Provider>
            </SidebarWrapper>
        </EntitySidebarContext.Provider>
    );
}

function useSelectedNode() {
    // Entity Profile sidebar, not lineage sidebar
    const { setSidebarClosed } = useContext(EntitySidebarContext);
    const [selectedNode, setSelectedNode] = useState<LineageEntity | null>(null);

    useOnSelectionChange({
        onChange: ({ nodes }) => {
            if (nodes.length) setSidebarClosed(true);
            setSelectedNode(nodes.length ? nodes[nodes.length - 1].data : null);
        },
    });

    return selectedNode;
}

function useQueryDetails(selectedNode: LineageEntity | null): EntitySidebarQueryDetails | undefined {
    const { nodes, adjacencyList } = useContext(LineageNodesContext);
    const { columnQueryData } = useContext(LineageDisplayContext);

    if (selectedNode?.type === EntityType.Query) {
        const data = columnQueryData.get(selectedNode.id);
        return {
            inputTables: getChildTableNames(nodes, selectedNode.urn, adjacencyList.UPSTREAM),
            outputTables: getChildTableNames(nodes, selectedNode.urn, adjacencyList.DOWNSTREAM),
            inputColumns: getColumnNames(nodes, data?.inputColumns),
            outputColumns: getColumnNames(nodes, data?.outputColumns),
            transformOperation: data?.transformOperation,
        };
    }
    return undefined;
}

function getChildTableNames(nodes: Map<string, LineageEntity>, urn: string, childMap: NeighborMap): string[] {
    return Array.from(childMap.get(urn) || []).map((childUrn) => nodes.get(childUrn)?.entity?.name || childUrn);
}

// TODO: Clean this up
function getColumnNames(
    nodes: Map<string, LineageEntity>,
    columns?: Array<[string, string]>,
): Array<[string, string]> | undefined {
    return columns?.map(([urn, column]) => [nodes.get(urn)?.entity?.name || urn, column]);
}

import React, { useCallback, useContext, useState } from 'react';
import { useOnSelectionChange, useStore } from 'reactflow';
import styled from 'styled-components/macro';
import EntitySidebarContext, { EntitySidebarQueryDetails } from '../shared/EntitySidebarContext';
import CompactContext from '../shared/CompactContext';
import { useEntityRegistry } from '../useEntityRegistry';
import { ChildMap, LineageDisplayContext, LineageEntity, LineageNodesContext } from './common';
import { EntityType } from '../../types.generated';
import TabFullsizedContext from '../shared/TabFullsizedContext';

const SidebarWrapper = styled.div<{ distanceFromTop: number; isClosed: boolean }>`
    position: absolute;
    right: ${(props) => (props.isClosed ? '-50px' : '0')};
    top: 0;
    display: flex;
    flex-direction: column;
    z-index: 9999;
    height: 100vh;
    // height: calc(100vh - ${(props) => props.distanceFromTop}px);
    // top: ${(props) => props.distanceFromTop}px;
    // overflow-y: scroll;
    border-left: 1px solid #e8e8e8;
    // box shadow to left of sidebar
    box-shadow: ${(props) => (props.isClosed ? 'none' : '0px 0px 5px rgba(0, 0, 0, 0.08)')};

    && {
        /* Hide scrollbar for Chrome, Safari, and Opera */

        &::-webkit-scrollbar {
            display: none;
        }
    }
`;

export default function LineageSidebar() {
    const entityRegistry = useEntityRegistry();
    const selectedEntity = useSelectedNode();
    const resetSelectedElements = useStore((actions) => actions.resetSelectedElements);
    const [isClosed, setIsClosed] = useState(false);
    const queryDetails = useQueryDetails(selectedEntity);

    const width = Math.min(window.innerWidth * 0.4, 500);

    const setSidebarClosed = useCallback(
        (closed) => {
            if (closed) {
                setTimeout(resetSelectedElements, 200); // TODO: Don't hardcode this
            }
            setIsClosed(closed);
        },
        [resetSelectedElements],
    );

    if (!selectedEntity) {
        return null;
    }

    return (
        <EntitySidebarContext.Provider
            value={{
                isClosed,
                setSidebarClosed,
                width,
                extra: queryDetails,
            }}
        >
            <SidebarWrapper distanceFromTop={0} isClosed={isClosed}>
                <CompactContext.Provider key={selectedEntity.urn} value>
                    {entityRegistry.renderProfile(selectedEntity.type, selectedEntity.urn)}
                </CompactContext.Provider>
            </SidebarWrapper>
        </EntitySidebarContext.Provider>
    );
}

function useSelectedNode() {
    const { isTabFullsize, setTabFullsize } = useContext(TabFullsizedContext);
    const [selectedNode, setSelectedNode] = useState<LineageEntity | null>(null);

    useOnSelectionChange({
        onChange: ({ nodes }) => {
            if (nodes.length && !isTabFullsize) {
                setTabFullsize(true);
            }
            setSelectedNode(nodes.length ? nodes[nodes.length - 1].data : null);
        },
    });

    return selectedNode;
}

function useQueryDetails(selectedNode: LineageEntity | null): EntitySidebarQueryDetails | undefined {
    const { nodes } = useContext(LineageNodesContext);
    const { childMaps, columnQueryData } = useContext(LineageDisplayContext);

    if (selectedNode?.type === EntityType.Query) {
        const data = columnQueryData.get(selectedNode.id);
        return {
            inputTables: getChildTableNames(nodes, selectedNode.urn, childMaps.UPSTREAM),
            outputTables: getChildTableNames(nodes, selectedNode.urn, childMaps.DOWNSTREAM),
            inputColumns: getColumnNames(nodes, data?.inputColumns),
            outputColumns: getColumnNames(nodes, data?.outputColumns),
            transformOperation: data?.transformOperation,
        };
    }
    return undefined;
}

function getChildTableNames(nodes: Map<string, LineageEntity>, urn: string, childMap: ChildMap): string[] {
    return Array.from(childMap.get(urn) || []).map((childUrn) => nodes.get(childUrn)?.entity?.name || childUrn);
}

// TODO: Clean this up
function getColumnNames(
    nodes: Map<string, LineageEntity>,
    columns?: Array<[string, string]>,
): Array<[string, string]> | undefined {
    return columns?.map(([urn, column]) => [nodes.get(urn)?.entity?.name || urn, column]);
}

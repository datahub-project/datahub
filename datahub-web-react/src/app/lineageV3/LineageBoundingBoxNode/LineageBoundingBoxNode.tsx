import React, { useCallback, useContext, useEffect, useState } from 'react';
import { useHistory, useLocation } from 'react-router-dom';
import { NodeProps, NodeResizer, useReactFlow, useStore } from 'reactflow';
import styled from 'styled-components';

import { IconStyleType } from '@app/entityV2/Entity';
import HomePill from '@app/lineageV3/LineageEntityNode/HomePill';
import LineageVisualizationContext from '@app/lineageV3/LineageVisualizationContext';
import NodeWrapper from '@app/lineageV3/NodeWrapper';
import {
    LINEAGE_NODE_HEIGHT,
    LINEAGE_NODE_WIDTH,
    LineageBoundingBox,
    LineageNodesContext,
    isGhostEntity,
    useIgnoreSchemaFieldStatus,
} from '@app/lineageV3/common';
import LineageCard from '@app/lineageV3/components/LineageCard';
import { getLineageUrl } from '@app/lineageV3/utils/lineageUtils';
import usePrevious from '@app/shared/usePrevious';
import { useEntityRegistry } from '@app/useEntityRegistry';

export const LINEAGE_BOUNDING_BOX_NODE_NAME = 'lineage-bounding-box';
export const BOUNDING_BOX_PADDING = 50;

const StyledNodeWrapper = styled(NodeWrapper)<{ $colorHex?: string }>`
    background-color: ${({ $colorHex, theme }) => ($colorHex ? `${$colorHex}30` : `${theme.colors.bgSurfaceBrand}50`)};
    border-top-left-radius: 0;

    align-items: start;
    width: 100%;
    height: 100%;

    transform: none;
`;

const CardWrapper = styled(NodeWrapper)`
    box-shadow: none;
    border-bottom-left-radius: 0;
    border-bottom-right-radius: 0;
    ${({ selected, theme }) => selected && `border-bottom: 1px solid ${theme.colors.border};`};

    position: absolute;
    left: 0;
    transform: translateY(-100%);
`;

const HomeIndicatorWrapper = styled.div<{ $cardHeight: number }>`
    display: flex;
    align-items: center;
    justify-content: center;

    position: absolute;
    top: ${({ $cardHeight }) => -$cardHeight - 21}px;
    left: 12px;
    z-index: -1;
`;

/**
 * Resizes the bounding box to fit its member nodes as they change size or move at runtime, e.g.
 * when a member's columns are expanded or a member is pushed down by `useAvoidIntersections`.
 * The computed layout only accounts for members' initial positions and default dimensions.
 */
function useFitToContents(id: string) {
    const { setNodes } = useReactFlow();
    // String key so the box is only resized when a member's position or size actually changes
    const contentsKey = useStore((state) =>
        state
            .getNodes()
            .filter((node) => node.parentId === id)
            .map((node) => `${node.position.x},${node.position.y},${node.width},${node.height}`)
            .join(';'),
    );

    useEffect(() => {
        if (!contentsKey) return;
        setNodes((nodes) => {
            const members = nodes.filter((node) => node.parentId === id);
            if (!members.length) return nodes;
            const width = Math.max(
                LINEAGE_NODE_WIDTH + 2 * BOUNDING_BOX_PADDING,
                ...members.map((node) => node.position.x + (node.width ?? LINEAGE_NODE_WIDTH) + BOUNDING_BOX_PADDING),
            );
            const height = Math.max(
                LINEAGE_NODE_HEIGHT + 2 * BOUNDING_BOX_PADDING,
                ...members.map((node) => node.position.y + (node.height ?? LINEAGE_NODE_HEIGHT) + BOUNDING_BOX_PADDING),
            );
            return nodes.map((node) => {
                if (node.id !== id || (node.width === width && node.height === height)) return node;
                return { ...node, width, height, style: { ...node.style, width, height } };
            });
        });
    }, [id, contentsKey, setNodes]);
}

export default function LineageBoundingBoxNode(props: NodeProps<LineageBoundingBox>) {
    const { data, selected, dragging } = props;
    const { urn, type, entity, colorHex } = data;

    useFitToContents(props.id);

    const entityRegistry = useEntityRegistry();
    const { rootUrn } = useContext(LineageNodesContext);
    const { searchedEntity, setIsDraggingBoundingBox } = useContext(LineageVisualizationContext);
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();
    const history = useHistory();
    const location = useLocation();

    const wasDragging = usePrevious(dragging);

    const isGhost = isGhostEntity(entity, ignoreSchemaFieldStatus);
    const isSearchedEntity = searchedEntity === urn;

    useEffect(() => {
        if (dragging) {
            setIsDraggingBoundingBox(true);
        } else if (!dragging && wasDragging) {
            setIsDraggingBoundingBox(false);
        }
    }, [dragging, wasDragging, setIsDraggingBoundingBox]);

    const [cardHeight, setCardHeight] = useState(54);
    const ref = useCallback(
        (node: HTMLDivElement | null) => {
            if (urn === rootUrn && node) {
                const resizeObserver = new ResizeObserver(() => {
                    setCardHeight(node.clientHeight);
                });
                resizeObserver.observe(node);
            }
        },
        [urn, rootUrn],
    );

    return (
        <>
            <NodeResizer
                color="transparent"
                isVisible={selected}
                minWidth={LINEAGE_NODE_WIDTH + 2 * BOUNDING_BOX_PADDING}
                minHeight={LINEAGE_NODE_HEIGHT + 2 * BOUNDING_BOX_PADDING}
                handleStyle={{ border: 'none' }}
            />
            <StyledNodeWrapper
                urn={urn}
                selected={selected}
                dragging={dragging}
                isGhost={isGhost}
                isSearchedEntity={isSearchedEntity}
                $colorHex={colorHex}
            >
                {urn === rootUrn && (
                    <HomeIndicatorWrapper $cardHeight={cardHeight}>
                        <HomePill showText />
                    </HomeIndicatorWrapper>
                )}
                <CardWrapper
                    urn={urn}
                    selected={selected}
                    dragging={dragging}
                    isGhost={isGhost}
                    isSearchedEntity={isSearchedEntity}
                >
                    <LineageCard
                        ref={ref}
                        urn={urn}
                        type={type}
                        loading={!entity}
                        name={entity?.name || urn}
                        properties={entity?.genericEntityProperties}
                        typeIcon={entityRegistry.getIcon(type, 16, IconStyleType.ACCENT)}
                        platformIcons={entity?.icon ? [entity.icon] : []}
                        childrenOpen={false}
                        onDoubleClick={
                            isGhost ? undefined : () => history.push(getLineageUrl(urn, type, location, entityRegistry))
                        }
                    />
                </CardWrapper>
            </StyledNodeWrapper>
        </>
    );
}

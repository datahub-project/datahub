import React, { useCallback, useContext, useEffect, useState } from 'react';
import { useHistory, useLocation } from 'react-router-dom';
import { Node, NodeProps, NodeResizer, Position, useReactFlow, useStore } from 'reactflow';
import styled from 'styled-components';

import { IconStyleType } from '@app/entityV2/Entity';
import BoundingBoxMemberCount from '@app/lineageV3/LineageBoundingBoxNode/BoundingBoxMemberCount';
import { DATA_FLOW_LINEAGE_CONTROL_NODE_NAME } from '@app/lineageV3/LineageBoundingBoxNode/DataFlowLineageControlNode';
import HomePill from '@app/lineageV3/LineageEntityNode/HomePill';
import LineageVisualizationContext from '@app/lineageV3/LineageVisualizationContext';
import NodeWrapper from '@app/lineageV3/NodeWrapper';
import {
    LINEAGE_HANDLE_OFFSET,
    LINEAGE_NODE_HEIGHT,
    LINEAGE_NODE_WIDTH,
    LineageBoundingBox,
    LineageNodesContext,
    isGhostEntity,
    useIgnoreSchemaFieldStatus,
} from '@app/lineageV3/common';
import HorizontalHandle from '@app/lineageV3/components/HorizontalHandle';
import LineageCard from '@app/lineageV3/components/LineageCard';
import { getLineageUrl } from '@app/lineageV3/utils/lineageUtils';
import usePrevious from '@app/sharedV2/usePrevious';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

export const LINEAGE_BOUNDING_BOX_NODE_NAME = 'lineage-bounding-box';
export const BOUNDING_BOX_PADDING = 50;
/** The box's label card renders above its top edge, so layout must leave room for it there. */
export const BOUNDING_BOX_LABEL_HEIGHT = 54;
/** Level with the label card's title, as entity nodes' handles are with theirs. Relative to the box's top edge. */
export const BOUNDING_BOX_HANDLE_Y = LINEAGE_HANDLE_OFFSET - BOUNDING_BOX_LABEL_HEIGHT;

const StyledNodeWrapper = styled(NodeWrapper)<{ $colorHex?: string }>`
    background-color: ${({ $colorHex, theme }) => ($colorHex ? `${$colorHex}30` : `${theme.colors.bgSurfaceBrand}50`)};
    border-top-left-radius: 0;

    align-items: start;
    width: 100%;
    height: 100%;

    transform: none;
`;

// Full-width header strip above the box: the data product card on the left, the member counter on
// the right, vertically centered.
const CardWrapper = styled(NodeWrapper)`
    box-shadow: none;
    border-bottom-left-radius: 0;
    border-bottom-right-radius: 0;
    ${({ selected, theme }) => selected && `border-bottom: 1px solid ${theme.colors.border};`};

    position: absolute;
    left: 0;
    transform: translateY(-100%);

    width: 100%;
    flex-direction: row;
    align-items: center;
    justify-content: space-between;
`;

// Holds the card content; grows to fill the header so the counter is pushed to the right edge.
const HeaderCardSlot = styled.div`
    flex: 1 1 auto;
    min-width: 0;
`;

const MemberCountWrapper = styled.div`
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    padding-right: 12px;
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

function isBoxMember(node: Node, boxId: string): boolean {
    return node.parentId === boxId && node.type !== DATA_FLOW_LINEAGE_CONTROL_NODE_NAME;
}

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
            .filter((node) => isBoxMember(node, id))
            .map((node) => `${node.position.x},${node.position.y},${node.width},${node.height}`)
            .join(';'),
    );

    useEffect(() => {
        if (!contentsKey) return;
        setNodes((nodes) => {
            const members = nodes.filter((node) => isBoxMember(node, id));
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
            {type === EntityType.DataFlow && (
                <>
                    <HorizontalHandle
                        type="target"
                        position={Position.Left}
                        isConnectable={false}
                        style={{ top: BOUNDING_BOX_HANDLE_Y }}
                    />
                    <HorizontalHandle
                        type="source"
                        position={Position.Right}
                        isConnectable={false}
                        style={{ top: BOUNDING_BOX_HANDLE_Y }}
                    />
                </>
            )}
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
                    <HeaderCardSlot>
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
                                isGhost
                                    ? undefined
                                    : () => history.push(getLineageUrl(urn, type, location, entityRegistry))
                            }
                        />
                    </HeaderCardSlot>
                    {data.memberCount !== undefined && (
                        <MemberCountWrapper>
                            <BoundingBoxMemberCount urn={urn} memberCount={data.memberCount} />
                        </MemberCountWrapper>
                    )}
                </CardWrapper>
            </StyledNodeWrapper>
        </>
    );
}

import { colors } from '@components';
import React, { useCallback, useContext, useEffect, useState } from 'react';
import { NodeProps, NodeResizer } from 'reactflow';
import styled from 'styled-components';

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
import usePrevious from '@app/shared/usePrevious';

export const LINEAGE_BOUNDING_BOX_NODE_NAME = 'lineage-bounding-box';
export const BOUNDING_BOX_PADDING = 50;

const StyledNodeWrapper = styled(NodeWrapper)`
    background-color: ${colors.violet[0]}50;
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
    ${({ selected }) => selected && `border-bottom: 1px solid ${colors.gray[100]};`};

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

export default function LineageBoundingBoxNode(props: NodeProps<LineageBoundingBox>) {
    const { data, selected, dragging } = props;
    const { urn, type, entity } = data;

    const { rootUrn } = useContext(LineageNodesContext);
    const { searchedEntity, setIsDraggingBoundingBox } = useContext(LineageVisualizationContext);
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();

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
                        platformIcons={entity?.icon ? [entity.icon] : []}
                        childrenOpen={false}
                    />
                </CardWrapper>
            </StyledNodeWrapper>
        </>
    );
}

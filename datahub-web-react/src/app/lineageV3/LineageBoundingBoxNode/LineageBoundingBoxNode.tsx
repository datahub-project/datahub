import { colors } from '@components';
import React, { useContext, useEffect } from 'react';
import { NodeProps, NodeResizer } from 'reactflow';
import styled from 'styled-components';

import LineageVisualizationContext from '@app/lineageV3/LineageVisualizationContext';
import NodeWrapper from '@app/lineageV3/NodeWrapper';
import {
    LINEAGE_NODE_HEIGHT,
    LINEAGE_NODE_WIDTH,
    LineageBoundingBox,
    isGhostEntity,
    useIgnoreSchemaFieldStatus,
} from '@app/lineageV3/common';
import LineageCard from '@app/lineageV3/components/LineageCard';
import usePrevious from '@app/shared/usePrevious';

export const LINEAGE_BOUNDING_BOX_NODE_NAME = 'lineage-bounding-box';
export const BOUNDING_BOX_PADDING = 50;

const StyledNodeWrapper = styled(NodeWrapper)`
    background-color: ${colors.violet[0]}50;

    width: 100%;
    height: 100%;

    transform: none;
`;

const CardWrapper = styled(NodeWrapper)`
    box-shadow: none;
    border-bottom-left-radius: 0;
    border-bottom-right-radius: 0;
    border-bottom: none;

    transform: translateY(-100%);
`;

export default function LineageBoundingBoxNode(props: NodeProps<LineageBoundingBox>) {
    const { data, selected, dragging } = props;
    const { urn, type, entity } = data;

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

    return (
        <>
            <NodeResizer
                color="transparent"
                isVisible={selected}
                minWidth={LINEAGE_NODE_WIDTH + 2 * BOUNDING_BOX_PADDING}
                minHeight={LINEAGE_NODE_HEIGHT + 2 * BOUNDING_BOX_PADDING}
            />
            <StyledNodeWrapper
                urn={urn}
                selected={selected}
                dragging={dragging}
                isGhost={isGhost}
                isSearchedEntity={isSearchedEntity}
            >
                <CardWrapper
                    urn={urn}
                    selected={selected}
                    dragging={dragging}
                    isGhost={isGhost}
                    isSearchedEntity={isSearchedEntity}
                >
                    <LineageCard
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

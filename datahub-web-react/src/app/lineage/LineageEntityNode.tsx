import React from 'react';
import { Group } from '@vx/group';
import styled from 'styled-components';

import { EntityType } from '../../types.generated';
import { NodeData, Direction } from './types';

function truncate(input, length) {
    if (!input) return '';
    if (input.length > length) {
        return `${input.substring(0, length)}...`;
    }
    return input;
}

const width = 140;
const height = 80;
const centerX = -width / 2;
const centerY = -height / 2;

const PointerGroup = styled(Group)`
    cursor: pointer;
`;

export default function LineageEntityNode({
    node,
    isSelected,
    isHovered,
    onEntityClick,
    onHover,
    onExpandClick,
    direction,
    isCenterNode,
}: {
    node: { x: number; y: number; data: Omit<NodeData, 'children'> };
    isSelected: boolean;
    isHovered: boolean;
    isCenterNode: boolean;
    onEntityClick: (EntitySelectParams) => void;
    onHover: (EntitySelectParams) => void;
    onExpandClick: (LineageExpandParams) => void;
    direction: Direction;
}) {
    return (
        <PointerGroup data-testid={`node-${node.data.urn}-${direction}`} top={node.x} left={node.y}>
            {node.data.unexploredChildren && (
                <Group
                    onClick={() => {
                        onExpandClick({ urn: node.data.urn, type: EntityType.Dataset, direction });
                    }}
                >
                    <circle
                        fill="white"
                        cy={centerY + height / 2}
                        cx={direction === Direction.Upstream ? centerX - 10 : centerX + width + 10}
                        r="20"
                    />
                    <g
                        fill="grey"
                        transform={`translate(${
                            direction === Direction.Upstream ? -1 * width + 37 : width / 2 - 10
                        } -21.5) scale(0.04 0.04)`}
                    >
                        <path d="M512 64C264.6 64 64 264.6 64 512s200.6 448 448 448 448-200.6 448-448S759.4 64 512 64zm192 472c0 4.4-3.6 8-8 8H544v152c0 4.4-3.6 8-8 8h-48c-4.4 0-8-3.6-8-8V544H328c-4.4 0-8-3.6-8-8v-48c0-4.4 3.6-8 8-8h152V328c0-4.4 3.6-8 8-8h48c4.4 0 8 3.6 8 8v152h152c4.4 0 8 3.6 8 8v48z" />
                    </g>
                </Group>
            )}
            <Group
                onClick={() => {
                    onEntityClick({ urn: node.data.urn, type: EntityType.Dataset });
                }}
                onMouseOver={() => {
                    onHover({ urn: node.data.urn, type: EntityType.Dataset });
                }}
                onMouseOut={() => {
                    onHover(undefined);
                }}
            >
                <rect
                    height={height}
                    width={width}
                    y={centerY}
                    x={centerX}
                    fill="white"
                    // eslint-disable-next-line no-nested-ternary
                    stroke={isSelected ? 'blue' : isHovered ? 'lightblue' : 'black'}
                    strokeWidth={isCenterNode ? 4 : 2}
                    strokeOpacity={1}
                    rx={10}
                />
                <text dy=".33em" fontSize={14} fontFamily="Arial" textAnchor="middle" fill="black">
                    {truncate(node.data.name?.split('.').slice(-1)[0], 16)}
                </text>
            </Group>
        </PointerGroup>
    );
}

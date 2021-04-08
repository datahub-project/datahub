import React from 'react';
import { Group } from '@vx/group';
import { LinkHorizontal } from '@vx/shape';
import styled from 'styled-components';

import { NodeData, Direction } from './types';

function truncate(input, length) {
    if (!input) return '';
    if (input.length > length) {
        return `${input.substring(0, length)}...`;
    }
    return input;
}

export const width = 212;
export const height = 80;
const iconWidth = 42;
const iconHeight = 42;
const iconX = -width / 2 + 8;
const iconY = -iconHeight / 2;
const centerX = -width / 2;
const centerY = -height / 2;
const textX = iconX + iconWidth + 8;

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
    nodesToRenderByUrn,
}: {
    node: { x: number; y: number; data: Omit<NodeData, 'children'> };
    isSelected: boolean;
    isHovered: boolean;
    isCenterNode: boolean;
    onEntityClick: (EntitySelectParams) => void;
    onHover: (EntitySelectParams) => void;
    onExpandClick: (LineageExpandParams) => void;
    direction: Direction;
    nodesToRenderByUrn: { [key: string]: { x: number; y: number; data: Omit<NodeData, 'children'> }[] };
}) {
    const unexploredHiddenChildren =
        node?.data?.countercurrentChildrenUrns?.filter((urn) => !(urn in nodesToRenderByUrn))?.length || 0;

    return (
        <PointerGroup data-testid={`node-${node.data.urn}-${direction}`} top={node.x} left={node.y}>
            {unexploredHiddenChildren ? (
                <Group>
                    {[...Array(unexploredHiddenChildren)].map((_, index) => {
                        const link = {
                            source: {
                                x: 0,
                                y: direction === Direction.Upstream ? 70 : -70,
                            },
                            target: {
                                x: (0.5 / (index + 1)) * 80 * (index % 2 === 0 ? 1 : -1),
                                y: direction === Direction.Upstream ? 150 : -150,
                            },
                        };
                        return (
                            <LinkHorizontal
                                data={link}
                                stroke={`url(#gradient-${direction})`}
                                strokeWidth="1"
                                fill="none"
                            />
                        );
                    })}
                </Group>
            ) : null}
            {node.data.unexploredChildren ? (
                <Group
                    onClick={() => {
                        onExpandClick({ urn: node.data.urn, type: node.data.type, direction });
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
                            direction === Direction.Upstream ? centerX - 32 : width / 2 - 10
                        } -21.5) scale(0.04 0.04)`}
                    >
                        <path d="M512 64C264.6 64 64 264.6 64 512s200.6 448 448 448 448-200.6 448-448S759.4 64 512 64zm192 472c0 4.4-3.6 8-8 8H544v152c0 4.4-3.6 8-8 8h-48c-4.4 0-8-3.6-8-8V544H328c-4.4 0-8-3.6-8-8v-48c0-4.4 3.6-8 8-8h152V328c0-4.4 3.6-8 8-8h48c4.4 0 8 3.6 8 8v152h152c4.4 0 8 3.6 8 8v48z" />
                    </g>
                </Group>
            ) : null}
            <Group
                onClick={() => {
                    onEntityClick({ urn: node.data.urn, type: node.data.type });
                }}
                onMouseOver={() => {
                    onHover({ urn: node.data.urn, type: node.data.type });
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
                {node.data.icon ? (
                    <image href={node.data.icon} height={iconHeight} width={iconWidth} x={iconX} y={iconY} />
                ) : null}
                <text dy=".33em" x={textX} fontSize={16} fontFamily="Arial" textAnchor="start" fill="black">
                    {truncate(node.data.name?.split('.').slice(-1)[0], 16)}
                </text>
                {unexploredHiddenChildren && isHovered ? (
                    <text
                        dy=".33em"
                        dx={textX}
                        fontSize={16}
                        fontFamily="Arial"
                        textAnchor="middle"
                        fill="black"
                        y={centerY - 20}
                    >
                        {unexploredHiddenChildren} hidden {direction === Direction.Upstream ? 'downstream' : 'upstream'}{' '}
                        {unexploredHiddenChildren > 1 ? 'dependencies' : 'dependency'}
                    </text>
                ) : null}
            </Group>
        </PointerGroup>
    );
}

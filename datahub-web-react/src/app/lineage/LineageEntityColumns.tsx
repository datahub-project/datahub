import React, { useContext, useState } from 'react';
import { Group } from '@vx/group';
import styled from 'styled-components';
import { NodeData, EntitySelectParams, VizEdge } from './types';
import { ANTD_GRAY } from '../entity/shared/constants';
import { getTitleHeight } from './utils/titleUtils';
import { LineageExplorerContext } from './utils/LineageExplorerContext';
import { centerY, iconX, width } from './constants';
import ColumnNode from './ColumnNode';

const UnselectableText = styled.text`
    user-select: none;
`;

interface Props {
    node: { x: number; y: number; data: Omit<NodeData, 'children'> };
    edgesToRender: VizEdge[];
    onHover: (EntitySelectParams) => void;
}

export default function LineageEntityColumns({ node, edgesToRender, onHover }: Props) {
    const { expandTitles, collapsedColumnsNodes, setCollapsedColumnsNodes } = useContext(LineageExplorerContext);
    const [isHoveringHide, setIsHoveringHide] = useState(false);
    const [isHoveringShow, setIsHoveringShow] = useState(false);
    const areColumnsCollapsed = !!collapsedColumnsNodes[node?.data?.urn || 'noop'];

    const titleHeight = getTitleHeight(expandTitles ? node.data.expandedName || node.data.name : undefined);

    return (
        <>
            <rect x={iconX - 21} y={centerY + 55 + titleHeight} width={width - 2} height="0.25" stroke={ANTD_GRAY[6]} />
            {areColumnsCollapsed && (
                <Group
                    onClick={(e) => {
                        const newCollapsedNodes = { ...collapsedColumnsNodes };
                        delete newCollapsedNodes[node.data.urn || 'noop'];
                        setCollapsedColumnsNodes(newCollapsedNodes);
                        setIsHoveringShow(false);
                        e.stopPropagation();
                    }}
                    onMouseOver={(e) => {
                        setIsHoveringShow(true);
                        onHover(undefined);
                        e.stopPropagation();
                    }}
                    onMouseOut={() => {
                        setIsHoveringShow(false);
                    }}
                >
                    <rect
                        x={iconX - 21}
                        y={centerY + 57 + titleHeight}
                        width={width - 2}
                        height="28"
                        fill={isHoveringShow ? ANTD_GRAY[3] : 'white'}
                        ry="4"
                        rx="4"
                    />
                    <UnselectableText
                        dy=".33em"
                        x={iconX}
                        y={centerY + 70 + titleHeight}
                        fontSize={12}
                        fontFamily="Arial"
                        fill="#1890FF"
                    >
                        Show +
                    </UnselectableText>
                </Group>
            )}
            {!areColumnsCollapsed && (
                <Group>
                    {node.data.schemaMetadata?.fields.map((field, idx) => (
                        <ColumnNode
                            field={field}
                            index={idx}
                            node={node}
                            edgesToRender={edgesToRender}
                            titleHeight={titleHeight}
                            onHover={onHover}
                        />
                    ))}
                    <Group
                        onClick={(e) => {
                            const newCollapsedNodes = {
                                ...collapsedColumnsNodes,
                                [node?.data?.urn || 'noop']: true,
                            };
                            setCollapsedColumnsNodes(newCollapsedNodes);
                            setIsHoveringHide(false);
                            e.stopPropagation();
                        }}
                        onMouseOver={(e) => {
                            setIsHoveringHide(true);
                            onHover(undefined);
                            e.stopPropagation();
                        }}
                        onMouseOut={() => {
                            setIsHoveringHide(false);
                        }}
                    >
                        <rect
                            x={iconX - 21}
                            y={centerY + 60 + titleHeight + (node.data.schemaMetadata?.fields.length || 0) * 30}
                            width={width - 2}
                            height="29"
                            fill={isHoveringHide ? ANTD_GRAY[3] : 'transparent'}
                            ry="4"
                            rx="4"
                        />
                        <UnselectableText
                            dy=".33em"
                            x={iconX}
                            y={centerY + 75 + titleHeight + (node.data.schemaMetadata?.fields.length || 0) * 30}
                            fontSize={12}
                            fontFamily="Arial"
                            fill="#1890FF"
                        >
                            Hide -
                        </UnselectableText>
                    </Group>
                </Group>
            )}
        </>
    );
}

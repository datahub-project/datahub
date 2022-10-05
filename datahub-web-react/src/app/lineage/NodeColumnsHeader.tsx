import React, { useContext, useState } from 'react';
import { Group } from '@vx/group';
import styled from 'styled-components';
import { DownOutlined, UpOutlined } from '@ant-design/icons';
import { blue } from '@ant-design/colors';
import { NodeData, EntitySelectParams } from './types';
import { getTitleHeight } from './utils/titleUtils';
import { LineageExplorerContext } from './utils/LineageExplorerContext';
import { centerY, EXPAND_COLLAPSE_COLUMNS_TOGGLE_HEIGHT, iconX, width } from './constants';

const HeaderWrapper = styled.div`
    align-items: center;
    padding: 5px 20px 5px 0;
    display: flex;
    justify-content: space-between;
    height: 100%;
`;

const ExpandCollapseText = styled.span`
    align-items: center;
    color: ${blue[5]};
    display: flex;
    &:hover {
        color: ${blue[7]};
    }
    .anticon {
        font-size: 10px;
        margin-left: 5px;
    }
`;

interface Props {
    node: { x: number; y: number; data: Omit<NodeData, 'children'> };
    onHover: (params?: EntitySelectParams) => void;
}

export default function NodeColumnsHeader({ node, onHover }: Props) {
    const { expandTitles, collapsedColumnsNodes, setCollapsedColumnsNodes } = useContext(LineageExplorerContext);
    const areColumnsCollapsed = !!collapsedColumnsNodes[node?.data?.urn || 'noop'];
    const titleHeight = getTitleHeight(expandTitles ? node.data.expandedName || node.data.name : undefined);

    function expandColumns() {
        const newCollapsedNodes = { ...collapsedColumnsNodes };
        delete newCollapsedNodes[node.data.urn || 'noop'];
        setCollapsedColumnsNodes(newCollapsedNodes);
    }

    function collapseColumns() {
        const newCollapsedNodes = {
            ...collapsedColumnsNodes,
            [node?.data?.urn || 'noop']: true,
        };
        setCollapsedColumnsNodes(newCollapsedNodes);
    }

    return (
        <Group
            onClick={(e) => e.stopPropagation()}
            onMouseOver={(e) => {
                onHover(undefined);
                e.stopPropagation();
            }}
        >
            <foreignObject
                x={iconX}
                y={centerY + 60 + titleHeight}
                width={width - 21}
                height={EXPAND_COLLAPSE_COLUMNS_TOGGLE_HEIGHT}
            >
                <HeaderWrapper>
                    {areColumnsCollapsed ? (
                        <ExpandCollapseText onClick={expandColumns}>
                            Show&nbsp; <DownOutlined />
                        </ExpandCollapseText>
                    ) : (
                        <ExpandCollapseText onClick={collapseColumns}>
                            Hide&nbsp; <UpOutlined />
                        </ExpandCollapseText>
                    )}
                </HeaderWrapper>
            </foreignObject>
        </Group>
    );
}

import React, { useContext, useState } from 'react';
import { Button, Input } from 'antd';
import { Group } from '@visx/group';
import styled from 'styled-components';
import { DownOutlined, SearchOutlined, UpOutlined } from '@ant-design/icons';
import { blue } from '@ant-design/colors';
import { NodeData } from './types';
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

const StyledInput = styled(Input)`
    border-radius: 30px;
    height: 26px;
    padding: 2px 8px;
    width: 125px;
    input {
        font-size: 12px;
    }
`;

interface Props {
    node: { x: number; y: number; data: Omit<NodeData, 'children'> };
    filterText: string;
    setFilterText: (text: string) => void;
}

export default function NodeColumnsHeader({ node, filterText, setFilterText }: Props) {
    const [isSearchBarVisible, setIsSearchBarVisible] = useState(false);
    const { expandTitles, collapsedColumnsNodes, setCollapsedColumnsNodes } = useContext(LineageExplorerContext);
    const areColumnsCollapsed = !!collapsedColumnsNodes[node?.data?.urn || 'noop'];
    const titleHeight = getTitleHeight(expandTitles ? node.data.expandedName || node.data.name : undefined);

    function expandColumns(e: React.MouseEvent<HTMLSpanElement, MouseEvent>) {
        const newCollapsedNodes = { ...collapsedColumnsNodes };
        delete newCollapsedNodes[node.data.urn || 'noop'];
        setCollapsedColumnsNodes(newCollapsedNodes);
        e.stopPropagation();
    }

    function collapseColumns(e: React.MouseEvent<HTMLSpanElement, MouseEvent>) {
        const newCollapsedNodes = {
            ...collapsedColumnsNodes,
            [node?.data?.urn || 'noop']: true,
        };
        setCollapsedColumnsNodes(newCollapsedNodes);
        e.stopPropagation();
    }

    function hideIfSearchIsEmpty() {
        if (!filterText) {
            setIsSearchBarVisible(false);
        }
    }

    return (
        <Group>
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
                    {!areColumnsCollapsed && (
                        <foreignObject onClick={(e) => e.stopPropagation()}>
                            {isSearchBarVisible && (
                                <StyledInput
                                    defaultValue={filterText}
                                    placeholder="Find column..."
                                    onChange={(e) => setFilterText(e.target.value)}
                                    onBlur={hideIfSearchIsEmpty}
                                    allowClear
                                    autoFocus
                                />
                            )}
                            {!isSearchBarVisible && (
                                <Button type="text" size="small" onClick={() => setIsSearchBarVisible(true)}>
                                    <SearchOutlined />
                                </Button>
                            )}
                        </foreignObject>
                    )}
                </HeaderWrapper>
            </foreignObject>
        </Group>
    );
}

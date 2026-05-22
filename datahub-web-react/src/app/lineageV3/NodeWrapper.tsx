import React, { HTMLAttributes, forwardRef } from 'react';
import styled from 'styled-components';

import { LINEAGE_NODE_WIDTH } from '@app/lineageV3/common';

const Wrapper = styled.div<{
    selected: boolean;
    dragging: boolean;
    isGhost: boolean;
    isSearchedEntity: boolean;
    $isDataProduct?: boolean;
    $nodeColor?: string;
}>`
    width: ${LINEAGE_NODE_WIDTH}px;

    background-color: ${({ $isDataProduct, $nodeColor, theme }) =>
        $isDataProduct && $nodeColor ? `${$nodeColor}1e` : theme.colors.bg};
    border-radius: 12px;
    border: 1px solid
        ${({ selected, $isDataProduct, $nodeColor, theme }) => {
            if (selected) return theme.colors.borderSelected;
            if ($isDataProduct && $nodeColor) return `${$nodeColor}80`;
            return theme.colors.border;
        }};
    box-shadow: ${({ isSearchedEntity, theme }) =>
        isSearchedEntity ? `0 0 4px 4px ${theme.colors.border}` : theme.colors.shadowXs};

    display: flex;
    align-items: center;
    flex-direction: column;

    > * {
        opacity: ${({ isGhost }) => (isGhost ? 0.5 : 1)};
    }

    cursor: ${({ isGhost, dragging }) => {
        if (isGhost) return 'not-allowed';
        if (dragging) return 'grabbing';
        return 'pointer';
    }};
`;

interface Props extends HTMLAttributes<HTMLDivElement> {
    urn: string;
    selected: boolean;
    dragging: boolean;
    isGhost: boolean;
    isSearchedEntity: boolean;
    $isDataProduct?: boolean;
    $nodeColor?: string;
    children?: React.ReactNode;
}

/** Base component to wrap graph nodes */
const NodeWrapper = forwardRef<HTMLDivElement, Props>(({ urn, children, ...props }, ref) => {
    return (
        <Wrapper ref={ref} data-testid={`lineage-node-${urn}`} {...props}>
            {children}
        </Wrapper>
    );
});

export default NodeWrapper;

import React, { HTMLAttributes } from 'react';
import styled from 'styled-components';

import { LINEAGE_NODE_WIDTH } from '@app/lineageV3/common';

const Wrapper = styled.div<{
    selected: boolean;
    dragging: boolean;
    isGhost: boolean;
    isSearchedEntity: boolean;
}>`
    width: ${LINEAGE_NODE_WIDTH}px;

    background-color: ${(props) => props.theme.colors.bg};
    border-radius: 12px;
    border: 1px solid ${({ selected, theme }) => (selected ? theme.colors.textBrand : theme.colors.border)};
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
    children?: React.ReactNode;
}

/** Base component to wrap graph nodes */
export default function NodeWrapper({ urn, children, ...props }: Props) {
    return (
        <Wrapper data-testid={`lineage-node-${urn}`} {...props}>
            {children}
        </Wrapper>
    );
}

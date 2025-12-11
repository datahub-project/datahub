/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { colors } from '@components';
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

    background-color: ${colors.white};
    border-radius: 12px;
    border: 1px solid ${({ selected }) => (selected ? colors.violet[600] : colors.gray[100])};
    box-shadow: ${({ isSearchedEntity }) =>
        isSearchedEntity ? `0 0 4px 4px ${colors.gray[100]}` : '0px 1px 2px 0px rgba(33, 23, 95, 0.07)'};

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

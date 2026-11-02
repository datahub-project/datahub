import { CircleNotch } from '@phosphor-icons/react/dist/csr/CircleNotch';
import React from 'react';
import styled, { keyframes } from 'styled-components';

import { CountText, SideControlWrapper } from '@app/lineageV3/LineageEntityNode/components';
import { ShownRelatedCounts } from '@app/lineageV3/common';
import { ColumnAsset } from '@app/lineageV3/types';

import { LineageDirection } from '@types';

const ControlWrapper = styled(SideControlWrapper)<{ direction: LineageDirection }>`
    ${({ direction }) =>
        direction === LineageDirection.Upstream ? 'right: calc(100% + 10px);' : 'left: calc(100% + 10px);'}
    top: 50%;

    // Neither brand color nor interactive, unlike the node's side controls: these counts are only a
    // readout for now. Both go away once there are controls behind them.
    color: ${(props) => props.theme.colors.textSecondary};
    pointer-events: none;
`;

// Matches the padding and height of the buttons inside the node's own side controls
const Counts = styled(CountText)`
    align-items: center;
    display: flex;
    gap: 3px;
    line-height: 18px;
    padding: 4px;
`;

const spin = keyframes`
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
`;

// Sized and colored to match the counts it stands in for, rather than the full-width,
// brand-colored alchemy `Loader`
const LoadingIndicator = styled(CircleNotch)`
    animation: ${spin} 1s linear infinite;
    height: 1em;
    width: 1em;
`;

interface Props {
    direction: LineageDirection;
    lineageAsset: ColumnAsset;
    shownRelated: ShownRelatedCounts;
}

/**
 * How much of a column's lineage in one direction is on the graph, e.g. `2 / 5`. Rendered to the
 * left and right of the hovered or selected column, and of every column related to it, in the
 * directions where its node may be hiding lineage -- see `mayHideLineage`. A loading indicator
 * stands in for the total until `getColumnLineageCounts` resolves; both numbers count columns the
 * way the graph draws them.
 *
 * TODO: Expand on hover into a panel of controls, as `ContractLineageControl` does.
 */
export function ColumnLineageControl({ direction, lineageAsset, shownRelated }: Props) {
    const numShown = shownRelated[direction];
    const total = direction === LineageDirection.Upstream ? lineageAsset.numUpstream : lineageAsset.numDownstream;

    if (numShown === undefined) {
        return null; // Lineage was never traversed this way, so there is nothing to say about it
    }
    if (total === 0) {
        return null; // The column has no lineage this way at all, so there is nothing to count
    }
    return (
        <ControlWrapper direction={direction} data-testid={`column-lineage-control-${lineageAsset.name}-${direction}`}>
            <Counts>
                {/* Counts are fetched per column, so they can lag behind the graph */}
                {total === undefined ? numShown : Math.min(numShown, total)} /{' '}
                {total === undefined ? <LoadingIndicator data-testid="column-lineage-count-loading" /> : total}
            </Counts>
        </ControlWrapper>
    );
}

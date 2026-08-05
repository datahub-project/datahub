import { LoadingOutlined } from '@ant-design/icons';
import { Spin } from 'antd';
import React from 'react';
import styled from 'styled-components';

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

const StyledLoadingIndicator = styled(LoadingOutlined)`
    display: flex;
    font-size: inherit;
`;

interface Props {
    direction: LineageDirection;
    lineageAsset: ColumnAsset;
    shownRelated: ShownRelatedCounts;
}

/**
 * How much of a column's lineage in one direction is missing from the graph, e.g. `2 / 5`. Rendered
 * to the left and right of the hovered or selected column, and of every column related to it, for
 * as long as it may have lineage it isn't showing. The total is only known once
 * `getColumnLineageCounts` resolves, so a loading indicator stands in for it until then; both
 * numbers count columns the way the graph draws them, so showing every related node clears it.
 *
 * TODO: Expand on hover into a panel of controls, as `ContractLineageControl` does.
 */
export function ColumnLineageControl({ direction, lineageAsset, shownRelated }: Props) {
    const numShown = shownRelated[direction];
    const numRelated = direction === LineageDirection.Upstream ? lineageAsset.numUpstream : lineageAsset.numDownstream;
    // Counts are marked fetched without running the query when all of the node's neighbors are
    // already on the graph, in which case what's shown is all there is
    const total = lineageAsset.lineageCountsFetched ? (numRelated ?? numShown ?? 0) : undefined;

    if (numShown === undefined) {
        return null; // Lineage was never traversed this way, so there is nothing to say about it
    }
    if (total !== undefined && total <= numShown) {
        return null; // Nothing hidden this way; while the total is unknown, there still might be
    }
    return (
        <ControlWrapper direction={direction} data-testid={`column-lineage-control-${lineageAsset.name}-${direction}`}>
            <Counts>
                {numShown} / {total === undefined ? <Spin indicator={<StyledLoadingIndicator />} /> : total}
            </Counts>
        </ControlWrapper>
    );
}

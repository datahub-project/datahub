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
 * How much of a column's lineage in one direction is on the graph, e.g. `2 / 5`. Rendered to the
 * left and right of the hovered or selected column, and of every column related to it, from the
 * moment `getColumnLineageCounts` is asked for its total until the column is no longer part of the
 * traversal -- a loading indicator stands in for the total in the meantime. Both numbers count
 * columns the way the graph draws them, so showing every related node brings them level.
 *
 * TODO: Expand on hover into a panel of controls, as `ContractLineageControl` does.
 */
export function ColumnLineageControl({ direction, lineageAsset, shownRelated }: Props) {
    const numShown = shownRelated[direction];
    const total = direction === LineageDirection.Upstream ? lineageAsset.numUpstream : lineageAsset.numDownstream;

    if (numShown === undefined) {
        return null; // Lineage was never traversed this way, so there is nothing to say about it
    }
    if (total === undefined && lineageAsset.lineageCountsFetched) {
        // Counts are marked fetched without querying when the node's every neighbor is on the
        // graph. Nothing was hidden to begin with, so there is no count worth showing.
        return null;
    }
    return (
        <ControlWrapper direction={direction} data-testid={`column-lineage-control-${lineageAsset.name}-${direction}`}>
            <Counts>
                {/* Cached counts can lag behind the graph, so never show more shown than exist */}
                {total === undefined ? numShown : Math.min(numShown, total)} /{' '}
                {total === undefined ? <Spin indicator={<StyledLoadingIndicator />} /> : total}
            </Counts>
        </ControlWrapper>
    );
}

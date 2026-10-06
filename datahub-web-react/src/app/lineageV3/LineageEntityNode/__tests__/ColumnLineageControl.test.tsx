import { render } from '@testing-library/react';
import React from 'react';

import { ColumnLineageControl } from '@app/lineageV3/LineageEntityNode/ColumnLineageControl';
import { ShownRelatedCounts } from '@app/lineageV3/common';
import { ColumnAsset, LineageAssetType } from '@app/lineageV3/types';
import CustomThemeProvider from '@src/CustomThemeProvider';

import { LineageDirection } from '@types';

const FIELD = 'id';
const TEST_ID = `column-lineage-control-${FIELD}-UPSTREAM`;

function renderControl(asset: Partial<ColumnAsset>, shownRelated: ShownRelatedCounts) {
    const lineageAsset: ColumnAsset = { name: FIELD, type: LineageAssetType.Column, ...asset };
    return render(
        <CustomThemeProvider>
            <ColumnLineageControl
                direction={LineageDirection.Upstream}
                lineageAsset={lineageAsset}
                shownRelated={shownRelated}
            />
        </CustomThemeProvider>,
    );
}

function upstream(numShown: number): ShownRelatedCounts {
    return { [LineageDirection.Upstream]: numShown };
}

describe('ColumnLineageControl', () => {
    it('shows a loading indicator in place of the total until counts are fetched', () => {
        const { getByTestId } = renderControl({}, upstream(1));

        expect(getByTestId(TEST_ID).textContent).toEqual('1 / ');
        expect(getByTestId('column-lineage-count-loading')).toBeTruthy();
    });

    it('shows how many related columns are on the graph out of the fetched total', () => {
        const { getByTestId } = renderControl({ numUpstream: 5, lineageCountsFetched: true }, upstream(2));

        expect(getByTestId(TEST_ID).textContent).toEqual('2 / 5');
    });

    it('keeps showing the counts once all of the column lineage is on the graph', () => {
        // The node still reports hidden lineage, or this control would not be rendered at all
        const { getByTestId } = renderControl({ numUpstream: 2, lineageCountsFetched: true }, upstream(2));

        expect(getByTestId(TEST_ID).textContent).toEqual('2 / 2');
    });

    it('never shows more as shown than exist, as per-column counts can lag behind the graph', () => {
        const { getByTestId } = renderControl({ numUpstream: 2, lineageCountsFetched: true }, upstream(3));

        expect(getByTestId(TEST_ID).textContent).toEqual('2 / 2');
    });

    it('renders nothing when the column has no lineage in this direction', () => {
        const { queryByTestId } = renderControl({ numUpstream: 0, lineageCountsFetched: true }, upstream(0));

        expect(queryByTestId(TEST_ID)).toBeNull();
    });

    it('renders nothing in a direction the traversal never explored', () => {
        const { queryByTestId } = renderControl(
            { numUpstream: 5, lineageCountsFetched: true },
            { [LineageDirection.Downstream]: 1 },
        );

        expect(queryByTestId(TEST_ID)).toBeNull();
    });
});

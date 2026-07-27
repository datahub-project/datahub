import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import { LineageTabContext } from '@app/entityV2/shared/tabs/Lineage/LineageTabContext';
import ColumnPathsText from '@app/previewV2/EntityPaths/ColumnPathsText';
import ColumnsRelationshipText from '@app/previewV2/EntityPaths/ColumnsRelationshipText';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { EntityPath, EntityType, LineageDirection } from '@types';

const RESULT_URN = 'urn:li:dataset:(result)';

// Minimal SchemaField entity — ColumnPathsText/DisplayedColumns only read type, fieldPath, parent.urn, urn.
const schemaField = (fieldPath: string, urn: string) =>
    ({ urn, type: EntityType.SchemaField, fieldPath, parent: { urn: RESULT_URN } }) as any;

function withContext(direction: LineageDirection, node: React.ReactNode) {
    return (
        <MockedProvider mocks={mocks} addTypename={false}>
            <TestPageContainer>
                <LineageTabContext.Provider
                    value={{
                        isColumnLevelLineage: true,
                        lineageDirection: direction,
                        selectedColumn: 'source_field',
                        lineageSearchPath: null,
                        setLineageSearchPath: () => {},
                    }}
                >
                    {node}
                </LineageTabContext.Provider>
            </TestPageContainer>
        </MockedProvider>
    );
}

describe('lineage column-path i18n', () => {
    describe('ColumnsRelationshipText — direction-dependent word order', () => {
        it('downstream renders {field} before {columns}', () => {
            const { container } = render(
                withContext(
                    LineageDirection.Downstream,
                    <ColumnsRelationshipText displayedColumns={[schemaField('col_a', 'urn:a')]} />,
                ),
            );
            expect(container.textContent).toMatch(/source_field\s*to\s*col_a/);
        });

        it('upstream renders {columns} before {field}', () => {
            const { container } = render(
                withContext(
                    LineageDirection.Upstream,
                    <ColumnsRelationshipText displayedColumns={[schemaField('col_a', 'urn:a')]} />,
                ),
            );
            expect(container.textContent).toMatch(/col_a\s*to\s*source_field/);
        });
    });

    describe('ColumnPathsText — direction + pluralized label', () => {
        const cases: Array<[LineageDirection, number, string]> = [
            [LineageDirection.Downstream, 1, 'Downstream column:'],
            [LineageDirection.Downstream, 2, 'Downstream columns:'],
            [LineageDirection.Upstream, 1, 'Upstream column:'],
            [LineageDirection.Upstream, 2, 'Upstream columns:'],
        ];

        it.each(cases)('%s with %i column(s) renders "%s"', (direction, count, expected) => {
            const cols = Array.from({ length: count }, (_, i) => schemaField(`col_${i}`, `urn:${i}`));
            const paths = [{ path: cols }] as unknown as EntityPath[];
            const { getByText } = render(
                withContext(
                    direction,
                    <ColumnPathsText paths={paths} resultEntityUrn={RESULT_URN} openModal={() => {}} />,
                ),
            );
            expect(getByText(expected)).toBeInTheDocument();
        });
    });
});

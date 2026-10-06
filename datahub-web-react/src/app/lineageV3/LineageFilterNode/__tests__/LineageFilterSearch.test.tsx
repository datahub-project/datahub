import { render } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import LineageFilterSearch from '@app/lineageV3/LineageFilterNode/LineageFilterSearch';
import { LineageFilter } from '@app/lineageV3/common';
import CustomThemeProvider from '@src/CustomThemeProvider';

import { useSearchAcrossLineageNamesQuery } from '@graphql/lineage.generated';
import { EntityType, LineageDirection } from '@types';

vi.mock('@graphql/lineage.generated', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@graphql/lineage.generated')>()),
    useSearchAcrossLineageNamesQuery: vi.fn(() => ({ loading: false })),
}));
vi.mock('@app/lineage/utils/useGetLineageTimeParams', () => ({
    useGetLineageTimeParams: () => ({ startTimeMillis: undefined, endTimeMillis: undefined }),
}));

const PARENT_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)';

describe('LineageFilterSearch', () => {
    it('searches through dbt and SQLMesh datasets and data jobs rather than counting them as hops', () => {
        const data = {
            id: 'filter',
            type: 'lineage-filter',
            parent: PARENT_URN,
            direction: LineageDirection.Upstream,
            limit: 10,
        } as unknown as LineageFilter;

        render(
            <CustomThemeProvider>
                <LineageFilterSearch data={data} numMatches={0} setNumMatches={() => {}} />
            </CustomThemeProvider>,
        );

        const options = vi.mocked(useSearchAcrossLineageNamesQuery).mock.calls[0][0];
        expect(options?.variables?.input.urn).toEqual(PARENT_URN);
        expect(options?.variables?.input.lineageFlags?.ignoreAsHops).toEqual([
            {
                entityType: EntityType.Dataset,
                platforms: ['urn:li:dataPlatform:dbt', 'urn:li:dataPlatform:sqlmesh'],
            },
            { entityType: EntityType.DataJob },
        ]);
    });
});

import { renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';

import useDownstreamQueries from '@app/entityV2/shared/tabs/Dataset/Queries/useDownstreamQueries';

import { useSearchAcrossLineageForQueriesQuery } from '@graphql/query.generated';
import { EntityType } from '@types';

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)';

vi.mock('@app/entity/shared/EntityContext', () => ({
    useBaseEntity: () => ({ dataset: { urn: DATASET_URN } }),
}));
vi.mock('@app/lineage/utils/useGetLineageTimeParams', () => ({
    useGetDefaultLineageStartTimeMillis: () => undefined,
}));
vi.mock('@graphql/query.generated', () => ({
    useSearchAcrossLineageForQueriesQuery: vi.fn(() => ({ data: undefined, loading: false })),
}));

describe('useDownstreamQueries', () => {
    it('walks through dbt and SQLMesh datasets and data jobs to reach downstream queries', () => {
        renderHook(() => useDownstreamQueries('', true));

        const options = vi.mocked(useSearchAcrossLineageForQueriesQuery).mock.calls[0][0];
        expect(options?.variables?.input.urn).toEqual(DATASET_URN);
        expect(options?.variables?.input.lineageFlags?.ignoreAsHops).toEqual([
            {
                entityType: EntityType.Dataset,
                platforms: ['urn:li:dataPlatform:dbt', 'urn:li:dataPlatform:sqlmesh'],
            },
            { entityType: EntityType.DataJob },
        ]);
        expect(options?.skip).toBe(false);
    });
});

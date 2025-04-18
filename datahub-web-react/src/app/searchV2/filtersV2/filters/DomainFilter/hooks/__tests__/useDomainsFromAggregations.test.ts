<<<<<<< HEAD
import { renderHook } from '@testing-library/react-hooks';

import useDomainsFromAggregations from '@app/searchV2/filtersV2/filters/DomainFilter/hooks/useDomainsFromAggregations';
import { EntityType } from '@src/types.generated';
=======
import { EntityType } from '@src/types.generated';
import { renderHook } from '@testing-library/react-hooks';
import useDomainsFromAggregations from '../useDomainsFromAggregations';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

describe('useDomainsFromAggregations', () => {
    it('should return domains from aggregations', () => {
        const domain = {
            type: EntityType.Domain,
            urn: 'test',
        };
        const aggregations = [
            {
                count: 1,
                value: 'test',
                entity: domain,
            },
        ];

        const response = renderHook(() => useDomainsFromAggregations(aggregations)).result.current;

        expect(response).toStrictEqual([domain]);
    });

    it('should ignore empty aggregations (count is zero)', () => {
        const domain = {
            type: EntityType.Domain,
            urn: 'test',
        };
        const aggregations = [
            {
                count: 0,
                value: 'test',
                entity: domain,
            },
        ];

        const response = renderHook(() => useDomainsFromAggregations(aggregations)).result.current;

        expect(response).toStrictEqual([]);
    });
});

import { renderHook } from '@testing-library/react-hooks';

import useTreeNodesFromDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useTreeNodesFromListDomains';
import { DomainItem } from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/types';

import { EntityType } from '@types';

describe('useTreeNodesFromDomains', () => {
    it('should return an empty array if domains is undefined', () => {
        const { result } = renderHook(() => useTreeNodesFromDomains());
        expect(result.current).toEqual([]);
    });

    it('should map domains to tree nodes', () => {
        const domains: DomainItem[] = [
            { urn: 'domain1', id: 'domain1', children: { total: 3 }, type: EntityType.Domain },
            { urn: 'domain2', id: 'domain2', type: EntityType.Domain },
        ];
        const { result } = renderHook(() => useTreeNodesFromDomains(domains));
        expect(result.current).toHaveLength(2);
        expect(result.current[0].value).toBe('domain1');
        expect(result.current[0].hasAsyncChildren).toBe(true);
        expect(result.current[1].hasAsyncChildren).toBe(false);
    });

    it('should memoize the result when domains do not change', () => {
        const domains: DomainItem[] = [{ urn: 'domain1', id: 'domain1', type: EntityType.Domain }];
        const { result, rerender } = renderHook(() => useTreeNodesFromDomains(domains));
        const firstResult = result.current;
        rerender(); // Re-run hook with same domains
        expect(result.current).toBe(firstResult); // Same reference due to useMemo
    });
});
